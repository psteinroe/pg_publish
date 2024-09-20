use async_trait::async_trait;
use tokio_postgres::types::PgLsn;

use super::{Store, StoreError};

#[derive(Debug)]
pub struct MemoryStore {
    lsn: PgLsn,
}

impl Default for MemoryStore {
    fn default() -> Self {
        Self {
            lsn: PgLsn::from(0),
        }
    }
}

#[async_trait]
impl Store for MemoryStore
{
    async fn write_lsn(&mut self, lsn: PgLsn) -> Result<(), StoreError> {
        self.lsn = lsn;
        Ok(())
    }

    async fn read_lsn(&self) -> Result<PgLsn, StoreError> {
        Ok(self.lsn)
    }
}

