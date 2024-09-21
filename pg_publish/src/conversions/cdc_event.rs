use postgres_protocol::message::backend::{
    BeginBody, CommitBody, DeleteBody, InsertBody, LogicalReplicationMessage, RelationBody,
    ReplicationMessage, TypeBody, UpdateBody,
};
use thiserror::Error;
use tracing::info;

use crate::pipeline::batching::BatchBoundary;

#[derive(Debug, Error)]
pub enum CdcEventConversionError {
    #[error("message not supported")]
    MessageNotSupported,

    #[error("unknown replication message")]
    UnknownReplicationMessage,
}

#[derive(Debug)]
pub enum CdcEvent {
    Type(TypeBody),
    Begin(BeginBody),
    Commit(CommitBody),
    Insert(InsertBody),
    Update(UpdateBody),
    Delete(DeleteBody),
    Relation(RelationBody),
    KeepAliveRequested { reply: bool },
}

impl BatchBoundary for CdcEvent {
    fn is_last_in_batch(&self) -> bool {
        matches!(
            self,
            // TODO in theory, this should lead to batches per commit, and all non-commit events should
            // be output directly
            CdcEvent::Commit(_) | CdcEvent::KeepAliveRequested { reply: _ }
        )
    }

    fn is_batch_relevant(&self) -> bool {
        matches!(
            self,
            CdcEvent::Insert(_) | CdcEvent::Update(_) | CdcEvent::Delete(_)
        )
    }
}

impl TryFrom<ReplicationMessage<LogicalReplicationMessage>> for CdcEvent {
    type Error = CdcEventConversionError;

    fn try_from(
        value: ReplicationMessage<LogicalReplicationMessage>,
    ) -> Result<CdcEvent, CdcEventConversionError> {
        match value {
            ReplicationMessage::XLogData(xlog_data) => match xlog_data.into_data() {
                LogicalReplicationMessage::Begin(begin_body) => Ok(CdcEvent::Begin(begin_body)),
                LogicalReplicationMessage::Commit(commit_body) => Ok(CdcEvent::Commit(commit_body)),
                LogicalReplicationMessage::Origin(_) => {
                    Err(CdcEventConversionError::MessageNotSupported)
                }
                LogicalReplicationMessage::Relation(relation_body) => {
                    Ok(CdcEvent::Relation(relation_body))
                }
                LogicalReplicationMessage::Type(type_body) => Ok(CdcEvent::Type(type_body)),
                LogicalReplicationMessage::Insert(insert_body) => {
                    info!("insert proto: {:?}", insert_body.tuple().tuple_data());
                    Ok(CdcEvent::Insert(insert_body))
                }
                LogicalReplicationMessage::Update(update_body) => Ok(CdcEvent::Update(update_body)),
                LogicalReplicationMessage::Delete(delete_body) => Ok(CdcEvent::Delete(delete_body)),
                LogicalReplicationMessage::Truncate(_) => {
                    Err(CdcEventConversionError::MessageNotSupported)
                }
                _ => Err(CdcEventConversionError::UnknownReplicationMessage),
            },
            ReplicationMessage::PrimaryKeepAlive(keep_alive) => Ok(CdcEvent::KeepAliveRequested {
                reply: keep_alive.reply() == 1,
            }),
            _ => Err(CdcEventConversionError::UnknownReplicationMessage),
        }
    }
}
