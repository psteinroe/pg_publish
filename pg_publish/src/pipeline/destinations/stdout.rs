use async_trait::async_trait;
use tracing::info;

use crate::conversions::cdc_operation_event::CdcOperationEvent;

use super::{Destination, DestinationError};

pub struct StdoutDestination;

#[async_trait]
impl Destination for StdoutDestination
where
    Self: Send + Sync,
{
    async fn send_batch(&self, events: Vec<CdcOperationEvent>) -> Result<(), DestinationError> {
        info!("sending batch {events:?}");
        Ok(())
    }

    fn max_batch_size(&self) -> usize {
        100
    }
}

