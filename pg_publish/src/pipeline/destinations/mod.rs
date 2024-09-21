pub mod stdout;

use async_trait::async_trait;
use thiserror::Error;

use crate::conversions::cdc_operation_event::CdcOperationEvent;

#[derive(Debug, Error)]
pub enum DestinationError {}

#[async_trait]
pub trait Destination: Send + Sync {
    async fn send_batch(&self, events: Vec<CdcOperationEvent>) -> Result<(), DestinationError>;

    fn max_batch_size(&self) -> usize;
}
