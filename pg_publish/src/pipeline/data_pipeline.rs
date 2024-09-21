use futures::StreamExt;
use tokio::pin;
use tokio_postgres::types::PgLsn;
use tracing::{error, info};

use crate::{
    conversions::{cdc_event::CdcEvent, cdc_operation_event::CdcOperationEvent},
    pipeline::{
        batching::{stream::TransactionBatchStream, BatchConfig},
        sources::{Source, SourceError},
        PipelineError,
    },
    table::TableSchema,
};

use super::{
    destinations::Destination,
    publisher::Publisher,
    stores::{store::StoreHandle, StoreAdapter},
};

pub struct DataPipeline<Src: Source, Dest: Destination, S: StoreAdapter + Sync + 'static> {
    source: Src,
    publisher: Publisher<Dest, S>,
    store_handle: StoreHandle<S>,
    batch_config: BatchConfig,
}

impl<Src: Source, Dest: Destination + 'static, S: StoreAdapter + 'static + Sync>
    DataPipeline<Src, Dest, S>
{
    pub async fn new(source: Src, destination: Dest, store: S) -> Result<Self, PipelineError> {
        let max_batch_size = destination.max_batch_size();

        let store_handle = StoreHandle::new(store).await?;

        let publisher = Publisher::new(store_handle.clone(), destination, 10);

        Ok(DataPipeline {
            batch_config: BatchConfig::new(max_batch_size),
            source,
            store_handle,
            publisher,
        })
    }

    pub async fn run(&mut self) -> Result<(), PipelineError> {
        let res = self.main_loop().await;

        error!("Main loop exited with {:?}", res);

        self.publisher.stop().await;

        info!("stopped publisher");

        self.store_handle.shutdown().await;

        info!("stopped store");

        res
    }

    async fn main_loop(&mut self) -> Result<(), PipelineError> {
        let cdc_events = self
            .source
            .get_cdc_stream(self.store_handle.get_latest_committed_lsn().await)
            .await?;

        // todo merge the streams
        pin!(cdc_events);

        let transaction_batch_stream =
            TransactionBatchStream::new(cdc_events, self.batch_config.clone());

        pin!(transaction_batch_stream);

        let mut new_last_lsn: PgLsn = PgLsn::from(0);
        let mut transaction_lsn: Option<PgLsn> = None;

        while let Some(batch) = transaction_batch_stream.next().await {
            let mut send_status_update = false;

            let mut op_events = Vec::with_capacity(batch.len());

            for event in batch {
                let event = event.map_err(SourceError::CdcStream)?;
                match event {
                    CdcEvent::Begin(begin_body) => {
                        let transaction_lsn_u64 = begin_body.final_lsn();
                        transaction_lsn = Some(transaction_lsn_u64.into());
                    }
                    CdcEvent::Commit(commit_body) => {
                        let commit_lsn: PgLsn = commit_body.commit_lsn().into();
                        if let Some(transaction_lsn) = transaction_lsn {
                            if commit_lsn == transaction_lsn {
                                new_last_lsn = commit_lsn;
                            } else {
                                Err(PipelineError::IncorrectCommitLsn(
                                    commit_lsn,
                                    transaction_lsn,
                                ))?
                            }
                        } else {
                            Err(PipelineError::CommitWithoutBegin)?
                        }
                    }
                    CdcEvent::Insert(insert_body) => {
                        let schema = self.source.get_table_schema(insert_body.rel_id())?;
                        let evt = CdcOperationEvent::from_insert_proto(insert_body, schema)?;
                        op_events.push(evt);
                    }
                    CdcEvent::Update(update_body) => {
                        let schema = self.source.get_table_schema(update_body.rel_id())?;
                        let evt = CdcOperationEvent::from_update_proto(update_body, schema)?;
                        op_events.push(evt);
                    }
                    CdcEvent::Delete(delete_body) => {
                        let schema = self.source.get_table_schema(delete_body.rel_id())?;
                        let evt = CdcOperationEvent::from_delete_proto(delete_body, schema)?;
                        op_events.push(evt);
                    }
                    CdcEvent::Relation(relation_body) => {
                        self.source
                            .set_table_schema(TableSchema::try_from(relation_body)?);
                    }
                    CdcEvent::KeepAliveRequested { reply } => {
                        send_status_update = reply;
                    }
                    _ => {}
                }
            }

            if !op_events.is_empty() {
                info!(
                    "publishing {:#?} events for lsn {}",
                    op_events, new_last_lsn
                );
                self.publisher.publish(new_last_lsn, op_events).await?;
            }

            if send_status_update {
                let last_commited_lsn = self.store_handle.get_latest_committed_lsn().await;
                info!("sending status update with lsn: {}", last_commited_lsn);
                let inner = unsafe {
                    transaction_batch_stream
                        .as_mut()
                        .get_unchecked_mut()
                        .get_inner_mut()
                };
                inner
                    .as_mut()
                    .send_status_update(last_commited_lsn)
                    .await
                    .map_err(|e| PipelineError::SourceError(SourceError::StatusUpdate(e)))?;
            }
        }

        Ok(())
    }
}
