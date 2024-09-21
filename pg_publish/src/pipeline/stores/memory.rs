use async_trait::async_trait;
use std::sync::Mutex;
use tokio_postgres::types::PgLsn;
use tracing::info;

use super::{StoreAdapter, StoreError};

#[derive(Debug)]
pub struct MemoryStore {
    lsn: Mutex<PgLsn>,
}

impl Clone for MemoryStore {
    fn clone(&self) -> Self {
        Self {
            lsn: Mutex::new(*self.lsn.lock().unwrap()),
        }
    }
}

impl Default for MemoryStore {
    fn default() -> Self {
        Self {
            lsn: Mutex::new(PgLsn::from(0)),
        }
    }
}

#[async_trait]
impl StoreAdapter for MemoryStore {
    async fn write_lsn(&self, lsn: PgLsn) -> Result<(), StoreError> {
        let mut guard = self.lsn.lock().unwrap();
        *guard = lsn;
        info!("stored {lsn}");
        Ok(())
    }

    async fn read_lsn(&self) -> Result<PgLsn, StoreError> {
        let guard = self.lsn.lock().unwrap();
        Ok(*guard)
    }
}
