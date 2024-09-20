use publisher::PublisherError;
use sources::SourceError;
use stores::StoreError;
use thiserror::Error;
use tokio_postgres::types::PgLsn;

use crate::conversions::cdc_operation_event::CdcOperationEventConversionError;

pub mod destinations;
pub mod data_pipeline;
pub mod sources;
pub mod batching;
pub mod publisher;
pub mod stores;

#[derive(Debug, Error)]
pub enum PipelineError {
    #[error("store error: {0}")]
    StoreError(#[from] StoreError),

    #[error("source error: {0}")]
    SourceError(#[from] SourceError),

    #[error("conversion error: {0}")]
    CdcOperationEventConversionError(#[from] CdcOperationEventConversionError),

    #[error("incorrect commit lsn: {0}(expected: {0})")]
    IncorrectCommitLsn(PgLsn, PgLsn),

    #[error("commit message without begin message")]
    CommitWithoutBegin,

    #[error("publisher error: {0}")]
    PublisherError(#[from] PublisherError),

    #[error("io error: {0}")]
    IOError(#[from] std::io::Error),
}


