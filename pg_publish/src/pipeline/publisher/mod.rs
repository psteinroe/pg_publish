use std::sync::{atomic::AtomicU64, Arc};

use thiserror::Error;
use tokio_util::task::TaskTracker;
use tokio_postgres::types::PgLsn;

use crate::conversions::cdc_operation_event::CdcOperationEvent;

use super::{destinations::{Destination, DestinationError}, stores::{Store, StoreError}};

#[derive(Debug, Error)]
pub enum PublisherError {
    #[error("destination error")]
    DestinationError(#[from] DestinationError),

    #[error("destination error")]
    StoreError(#[from] StoreError),
}

pub struct Publisher<Dest: Destination, Str: Store> {
    destination: Arc<Dest>,
    store: Str,
    last_lsn: Arc<AtomicU64>,
    trask_tracker: TaskTracker,
}

impl<Dest: Destination + 'static, Str: Store + 'static> Publisher<Dest, Str> {
    pub async fn new(destination: Dest, store: Str) -> Self {
        let last_lsn = store.read_lsn().await.unwrap_or(PgLsn::from(0));
        Self {
            destination: Arc::new(destination),
            store,
            last_lsn: Arc::new(AtomicU64::new(last_lsn.into())),
            trask_tracker: TaskTracker::new(),
        }
    }

    pub async fn read_lsn(&self) -> Result<PgLsn, StoreError> {
        self.store.read_lsn().await
    }

    pub fn publish(&mut self, commit_lsn: PgLsn, events: Vec<CdcOperationEvent>) -> Result<(), PublisherError> {
        // TODO make this async and apply backpressure if we currently process > x futures
        // block until we have capacity
        // todo apply backpressure if we have too many futures in flight

        let destination = self.destination.clone();
        let last_lsn = self.last_lsn.clone();

        self.trask_tracker.spawn(async move {
            // todo retry forever?
            if destination.send_batch(events).await.is_ok() {
                last_lsn.fetch_max(commit_lsn.into(), std::sync::atomic::Ordering::SeqCst);
            }

        });

        let _ =self.store.write_lsn(self.last_lsn.load(std::sync::atomic::Ordering::SeqCst).into());

        Ok(())
    }

    pub async fn flush(&mut self) {
        self.trask_tracker.wait().await;
        let _ = self.store.write_lsn(self.last_lsn.load(std::sync::atomic::Ordering::SeqCst).into()).await;
    }

    pub fn max_batch_size(&self) -> usize {
        self.destination.max_batch_size()
    }
}
