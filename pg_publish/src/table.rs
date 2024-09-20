use std::fmt::Display;

use tokio_postgres::types::Type;

use crate::escape::quote_identifier;

#[derive(Debug, Clone)]
pub struct TableName {
    pub schema: String,
    pub name: String,
}

impl TableName {
    pub fn as_quoted_identifier(&self) -> String {
        let quoted_schema = quote_identifier(&self.schema);
        let quoted_name = quote_identifier(&self.name);
        format!("{quoted_schema}.{quoted_name}")
    }
}

impl Display for TableName {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.write_fmt(format_args!("{0}.{1}", self.schema, self.name))
    }
}

pub type TableId = u32;

#[derive(Debug, Clone)]
pub struct ColumnSchema {
    pub name: String,
    pub type_id: i32,
}

#[derive(Debug, Clone)]
pub struct TableSchema {
    pub table_name: TableName,
    pub table_id: TableId,
    pub column_schemas: Vec<ColumnSchema>,
}

