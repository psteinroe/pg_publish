use async_trait::async_trait;
use thiserror::Error;
use tokio_postgres::types::PgLsn;

pub mod memory;

#[derive(Debug, Error)]
pub enum StoreError { }

#[async_trait]
pub trait Store {
    async fn write_lsn(&mut self, lsn: PgLsn) -> Result<(), StoreError>;

    async fn read_lsn(&self) -> Result<PgLsn, StoreError>;
}


