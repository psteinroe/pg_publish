use async_trait::async_trait;
use thiserror::Error;
use tokio_postgres::types::PgLsn;

pub mod memory;
pub mod store;

#[derive(Debug, Error)]
pub enum StoreError {}

#[async_trait]
pub trait StoreAdapter: Clone + Send {
    async fn write_lsn(&self, lsn: PgLsn) -> Result<(), StoreError>;

    async fn read_lsn(&self) -> Result<PgLsn, StoreError>;
}
