use async_trait::async_trait;
use postgres::{stream::{CdcStream, CdcStreamError, StatusUpdateError}, PostgresSourceError};
use thiserror::Error;
use tokio_postgres::types::PgLsn;

use crate::table::{TableId, TableSchema};


pub mod postgres;

#[derive(Debug, Error)]
pub enum SourceError {
    #[error("source error: {0}")]
    Postgres(#[from] PostgresSourceError),

    #[error("cdc stream error: {0}")]
    CdcStream(#[from] CdcStreamError),

    #[error("status update error: {0}")]
    StatusUpdate(#[from] StatusUpdateError),
}

#[async_trait]
pub trait Source {
    fn get_table_schema(&self, table_id: TableId) -> Result<&TableSchema, SourceError>;

    fn set_table_schema(&mut self, table_schema: TableSchema);

    async fn get_cdc_stream(&self, start_lsn: PgLsn) -> Result<CdcStream, SourceError>;
}


