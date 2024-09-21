use std::{collections::HashMap, str::from_utf8};

use postgres_protocol::message::backend::{
    DeleteBody as DeleteBodyProto, InsertBody as InsertBodyProto, TupleData,
    UpdateBody as UpdateBodyProto,
};
use thiserror::Error;
use tokio_postgres::types::Type;

use crate::table::{ColumnSchema, TableId, TableSchema};

use super::json_cell::{JsonConverter, JsonValueConversionError};

#[derive(Debug, Error)]
pub enum CdcOperationEventConversionError {
    #[error("old tuple not present")]
    MissingOldTuple,

    #[error("unchanged toast not yet supported")]
    UnchangedToastNotSupported,

    #[error("unable to convert tuple data to json")]
    InvalidData(#[from] JsonValueConversionError),
}

#[derive(Debug, PartialEq)]
pub enum Operation {
    INSERT,
    UPDATE,
    DELETE,
}

pub type TableRowRecord = HashMap<String, serde_json::Value>;

#[derive(Debug)]
pub struct InsertBody {
    pub tg_relid: TableId,
    pub tg_op: Operation,
    pub tg_table_name: String,
    pub tg_table_schema: String,

    pub new: TableRowRecord,
}

#[derive(Debug)]
pub struct UpdateBody {
    pub tg_relid: TableId,
    pub tg_op: Operation,
    pub tg_table_name: String,
    pub tg_table_schema: String,

    pub new: TableRowRecord,
    pub old: TableRowRecord,
}

#[derive(Debug)]
pub struct DeleteBody {
    pub tg_relid: TableId,
    pub tg_op: Operation,
    pub tg_table_name: String,
    pub tg_table_schema: String,

    pub old: TableRowRecord,
}

#[derive(Debug)]
pub enum CdcOperationEvent {
    Insert(InsertBody),
    Update(UpdateBody),
    Delete(DeleteBody),
}

impl CdcOperationEvent {
    fn convert_tuple_data_to_row(
        column_schemas: &[ColumnSchema],
        tuple_data: &[TupleData],
    ) -> Result<TableRowRecord, CdcOperationEventConversionError> {
        let mut values = HashMap::with_capacity(column_schemas.len());

        for (i, column_schema) in column_schemas.iter().enumerate() {
            let typ = match Type::from_oid(column_schema.type_id as u32) {
                Some(t) => t,
                None => Type::TEXT,
            };
            values.insert(
                column_schema.name.to_string(),
                JsonConverter::from_tuple_data(&typ, &tuple_data[i])?,
            );
        }

        Ok(values)
    }

    pub fn from_insert_proto(
        evt: InsertBodyProto,
        table_schema: &TableSchema,
    ) -> Result<CdcOperationEvent, CdcOperationEventConversionError> {
        Ok(CdcOperationEvent::Insert(InsertBody {
            tg_relid: evt.rel_id(),
            tg_op: Operation::INSERT,
            tg_table_name: table_schema.table_name.name.clone(),
            tg_table_schema: table_schema.table_name.schema.clone(),

            new: Self::convert_tuple_data_to_row(
                &table_schema.column_schemas,
                evt.tuple().tuple_data(),
            )?,
        }))
    }

    pub fn from_update_proto(
        evt: UpdateBodyProto,
        table_schema: &TableSchema,
    ) -> Result<CdcOperationEvent, CdcOperationEventConversionError> {
        let old = evt
            .old_tuple()
            .ok_or(CdcOperationEventConversionError::MissingOldTuple)?;
        Ok(CdcOperationEvent::Update(UpdateBody {
            tg_relid: evt.rel_id(),
            tg_op: Operation::UPDATE,
            tg_table_name: table_schema.table_name.name.clone(),
            tg_table_schema: table_schema.table_name.schema.clone(),

            new: Self::convert_tuple_data_to_row(
                &table_schema.column_schemas,
                evt.new_tuple().tuple_data(),
            )?,
            old: Self::convert_tuple_data_to_row(&table_schema.column_schemas, old.tuple_data())?,
        }))
    }

    pub fn from_delete_proto(
        evt: DeleteBodyProto,
        table_schema: &TableSchema,
    ) -> Result<CdcOperationEvent, CdcOperationEventConversionError> {
        let old = evt
            .old_tuple()
            .ok_or(CdcOperationEventConversionError::MissingOldTuple)?;

        Ok(CdcOperationEvent::Delete(DeleteBody {
            tg_relid: evt.rel_id(),
            tg_op: Operation::DELETE,
            tg_table_name: table_schema.table_name.name.clone(),
            tg_table_schema: table_schema.table_name.schema.clone(),

            old: Self::convert_tuple_data_to_row(&table_schema.column_schemas, old.tuple_data())?,
        }))
    }
}
