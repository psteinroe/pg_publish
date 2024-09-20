use futures::StreamExt;
use tokio::pin;
use tokio_postgres::types::PgLsn;
use tracing::info;

use crate::{
    conversions::{cdc_event::CdcEvent, cdc_operation_event::CdcOperationEvent},
    pipeline::{
        batching::{stream::TransactionBatchStream, BatchConfig},
        sources::{Source, SourceError},
        PipelineError,
    }, table::TableSchema,
};

use super::{destinations::Destination, publisher::Publisher, stores::Store};

pub struct DataPipeline<Src: Source, Dest: Destination, Str: Store> {
    source: Src,
    publisher: Publisher<Dest, Str>,

    batch_config: BatchConfig,
}

impl<Src: Source, Dest: Destination + 'static, Str: Store + 'static> DataPipeline<Src, Dest, Str> {
    pub fn new(source: Src, publisher: Publisher<Dest, Str>) -> Self {
        DataPipeline { batch_config: BatchConfig::new(publisher.max_batch_size()), source, publisher }
    }

    pub async fn run(&mut self) -> Result<(), PipelineError> {
        let res = self.main_loop(self.publisher.read_lsn().await?).await;
        println!("DataPipeline::run() res: {:?}", res);

        self.publisher.flush().await;

        println!("DataPipeline::run() flushed publisher");

        res
    }

    async fn main_loop(&mut self, start_lsn: PgLsn) -> Result<(), PipelineError> {
        let start_lsn: u64 = start_lsn.into();

        let cdc_events = self.source.get_cdc_stream((start_lsn + 1).into()).await?;

        pin!(cdc_events);

        let transaction_batch_stream = TransactionBatchStream::new(cdc_events, self.batch_config.clone());

        pin!(transaction_batch_stream);

        let mut new_last_lsn: PgLsn = PgLsn::from(0);
        let mut transaction_lsn: Option<PgLsn> = None;

        info!("starting main loop");

        while let Some(batch) = transaction_batch_stream.next().await {
            info!("processing batch {:#?}", batch);

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
                                Err(PipelineError::IncorrectCommitLsn(commit_lsn, transaction_lsn))?
                            }
                        } else {
                            Err(PipelineError::CommitWithoutBegin)?
                        }
                    }
                    CdcEvent::Insert(insert_body) => {
                        let schema = self.source.get_table_schema(insert_body.rel_id())?;
                        let evt = CdcOperationEvent::from_insert_proto(insert_body, schema)?;
                        op_events.push(evt);
                    },
                    CdcEvent::Update(update_body) => {
                        let schema = self.source.get_table_schema(update_body.rel_id())?;
                        let evt = CdcOperationEvent::from_update_proto(update_body, schema)?;
                        op_events.push(evt);
                    },
                    CdcEvent::Delete(delete_body) => {
                        let schema = self.source.get_table_schema(delete_body.rel_id())?;
                        let evt = CdcOperationEvent::from_delete_proto(delete_body, schema)?;
                        op_events.push(evt);
                    },
                    CdcEvent::Relation(relation_body) => {
                        self.source.set_table_schema(TableSchema::try_from(relation_body)?);
                    },
                    CdcEvent::KeepAliveRequested { reply } => {
                        send_status_update = reply;
                    }
                    _ => {}
                }
            }

            info!("publishing {:#?} events", op_events);

            if !op_events.is_empty() {
                self.publisher.publish(new_last_lsn, op_events)?;
            }

            if send_status_update {
                let last_lsn = self.publisher.read_lsn().await?;
                info!("sending status update with lsn: {last_lsn}");
                let inner = unsafe {
                   transaction_batch_stream
                        .as_mut()
                        .get_unchecked_mut()
                        .get_inner_mut()
                };
                inner
                    .as_mut()
                    .send_status_update(last_lsn)
                    .await
                    .map_err(|e| PipelineError::SourceError(SourceError::StatusUpdate(e)))?;
            }
        }

        Ok(())
    }
}
