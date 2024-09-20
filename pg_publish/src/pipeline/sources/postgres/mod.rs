use std::{
    collections::HashMap,
    time::{Duration, UNIX_EPOCH},
};

use async_trait::async_trait;
use stream::CdcStream;
use thiserror::Error;
use tokio_postgres::types::PgLsn;
use tracing::{error, info};

use crate::{
    clients::postgres::{ReplicationClient, ReplicationClientError},
    table::{TableId, TableName, TableSchema},
};

use super::{Source, SourceError};

pub mod stream;

pub enum TableNamesFrom {
    Publication(String),
}

#[derive(Debug, Error)]
pub enum PostgresSourceError {
    #[error("schema missing for table id {0}")]
    MissingSchema(TableId),

    #[error("replication client error: {0}")]
    ReplicationClient(#[from] ReplicationClientError),

    #[error("cdc stream can only be started with a publication")]
    MissingPublication,

    #[error("cdc stream can only be started with a slot_name")]
    MissingSlotName,
}

pub struct PostgresSource {
    pub replication_client: ReplicationClient,
    table_schemas: HashMap<TableId, TableSchema>,
    slot_name: Option<String>,
    publication: Option<String>,
}

impl PostgresSource {
    pub async fn new(
        host: &str,
        port: u16,
        database: &str,
        username: &str,
        password: Option<String>,
        slot_name: Option<String>,
        table_names_from: TableNamesFrom,
    ) -> Result<PostgresSource, PostgresSourceError> {
        let replication_client =
            ReplicationClient::connect_no_tls(host, port, database, username, password).await?;

        replication_client.begin_readonly_transaction().await?;

        if let Some(ref slot_name) = slot_name {
            replication_client.get_or_create_slot(slot_name).await?;
        }
        let (table_names, publication) =
            Self::get_table_names_and_publication(&replication_client, table_names_from).await?;
        let table_schemas = replication_client.get_table_schemas(&table_names).await?;

        replication_client.commit_txn().await?;

        Ok(PostgresSource {
            replication_client,
            table_schemas,
            publication,
            slot_name,
        })
    }

    fn publication(&self) -> Option<&String> {
        self.publication.as_ref()
    }

    fn slot_name(&self) -> Option<&String> {
        self.slot_name.as_ref()
    }

    async fn get_table_names_and_publication(
        replication_client: &ReplicationClient,
        table_names_from: TableNamesFrom,
    ) -> Result<(Vec<TableName>, Option<String>), ReplicationClientError> {
        Ok(match table_names_from {
            TableNamesFrom::Publication(publication) => (
                replication_client
                    .get_publication_table_names(&publication)
                    .await?,
                Some(publication),
            ),
        })
    }
}

#[async_trait]
impl Source for PostgresSource {
    fn get_table_schema(&self, table_id: TableId) -> Result<&TableSchema, SourceError> {
        let r = self
            .table_schemas
            .get(&table_id)
            .ok_or(PostgresSourceError::MissingSchema(table_id))?;
        Ok(r)
    }

    fn set_table_schema(&mut self, table_schema: TableSchema) {
        self.table_schemas
            .insert(table_schema.table_id, table_schema);
    }

    async fn get_cdc_stream(&self, start_lsn: PgLsn) -> Result<CdcStream, SourceError> {
        info!("starting cdc stream at lsn {start_lsn}");
        let publication = self
            .publication()
            .ok_or(PostgresSourceError::MissingPublication)?;
        let slot_name = self
            .slot_name()
            .ok_or(PostgresSourceError::MissingSlotName)?;
        let stream = self
            .replication_client
            .get_logical_replication_stream(publication, slot_name, start_lsn)
            .await
            .map_err(PostgresSourceError::ReplicationClient)?;

        const TIME_SEC_CONVERSION: u64 = 946_684_800;
        let postgres_epoch = UNIX_EPOCH + Duration::from_secs(TIME_SEC_CONVERSION);

        Ok(CdcStream::new(stream, postgres_epoch))
    }
}
