use postgres_protocol::message::backend::RelationBody;

use crate::table::{ColumnSchema, TableName, TableSchema};

impl TryFrom<RelationBody> for TableSchema {
    type Error = std::io::Error;

    fn try_from(relation: RelationBody) -> Result<Self, Self::Error> {
        let schema = if relation.namespace()?.is_empty() {
            "pg_catalog".to_string()
        } else {
            relation.namespace()?.to_string()
        };

        let mut column_schemas = vec![];
        for c in relation.columns() {
            column_schemas.push(ColumnSchema {
                name: c.name()?.to_string(),
                type_id: c.type_id(),
            });
        }

        Ok(TableSchema {
            table_name: TableName {
                schema,
                name: relation.name()?.to_string(),
            },
            table_id: relation.rel_id(),
            column_schemas,
        })
    }
}
