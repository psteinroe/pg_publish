use std::sync::Arc;

use thiserror::Error;
use tokio::sync::{mpsc, AcquireError, Semaphore};
use tokio_postgres::types::PgLsn;
use tokio_util::task::TaskTracker;
use tracing::{error, info};

use crate::conversions::cdc_operation_event::CdcOperationEvent;

use super::{
    destinations::Destination,
    stores::{store::StoreHandle, StoreAdapter},
};

#[derive(Debug, Error)]
pub enum PublisherError {
    #[error("unable to acquire sempahore")]
    SemaphoreError(#[from] AcquireError),
}

pub struct Publisher<Dest: Destination, A: StoreAdapter + Sync + 'static> {
    store_handle: StoreHandle<A>,
    semaphore: Arc<Semaphore>,
    destination: Arc<Dest>,
    task_tracker: TaskTracker,
}

impl<Dest: Destination + 'static, A: StoreAdapter + Sync + 'static> Publisher<Dest, A> {
    pub fn new(store_handle: StoreHandle<A>, destination: Dest, max_concurrency: usize) -> Self {
        Publisher {
            store_handle,
            semaphore: Arc::new(Semaphore::new(max_concurrency)),
            destination: Arc::new(destination),
            task_tracker: TaskTracker::new(),
        }
    }

    pub async fn stop(&self) {
        self.semaphore.close();
        self.task_tracker.close();
        self.task_tracker.wait().await;
    }

    pub async fn publish(
        &self,
        lsn: PgLsn,
        data: Vec<CdcOperationEvent>,
    ) -> Result<(), PublisherError> {
        let permit = self.semaphore.clone().acquire_owned().await?;
        let d = self.destination.clone();
        let store = self.store_handle.clone();

        self.task_tracker.spawn(async move {
            // todo retry forever?
            let res = d.send_batch(data).await;

            match res {
                Ok(_) => {
                    info!("sending new last lsn to store {}", lsn);
                    store.report_lsn(lsn).await;
                }
                Err(e) => {
                    error!("Error: {:?}", e);
                }
            }

            drop(permit);
            info!("Released permit");
        });

        Ok(())
    }
}
