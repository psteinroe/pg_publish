use std::{
    num::ParseIntError,
    str::{from_utf8, ParseBoolError, Utf8Error},
};

use postgres_protocol::message::backend::TupleData;
use thiserror::Error;
use tokio_postgres::types::{FromSql, Type};

pub type JsonValue = serde_json::Value;

pub struct JsonConverter;

#[derive(Debug, Error)]
pub enum JsonValueConversionError {
    #[error("type is not supported {0}")]
    UnsupportedType(String),

    #[error("unchanged toast not supported")]
    UnchangedToastNotSupported,

    #[error("invalid string value")]
    InvalidStr(#[from] Utf8Error),

    #[error("invalid bool value")]
    InvalidBool(#[from] ParseBoolError),

    #[error("invalid int value")]
    InvalidInt(#[from] ParseIntError),

    #[error("invalid timestamp value")]
    InvalidTimestamp(#[from] chrono::ParseError),
}

impl JsonConverter {
    pub fn from_tuple_data(
        typ: &Type,
        val: &TupleData,
    ) -> Result<JsonValue, JsonValueConversionError> {
        let bytes = match val {
            TupleData::Null => {
                return Ok(JsonValue::Null);
            }
            TupleData::UnchangedToast => {
                return Err(JsonValueConversionError::UnchangedToastNotSupported)
            }
            TupleData::Text(bytes) => &bytes[..],
        };
        match *typ {
            Type::BOOL => {
                let val = from_utf8(bytes)?;
                let parsed = if val == "t" {
                    true
                } else if val == "f" {
                    false
                } else {
                    val.parse()?
                };
                Ok(JsonValue::Bool(parsed))
            }
            Type::CHAR
            | Type::BPCHAR
            | Type::VARCHAR
            | Type::NAME
            | Type::TEXT
            | Type::TIMESTAMP
            | Type::UUID
            | Type::TIMESTAMPTZ => {
                let val = from_utf8(bytes)?;
                Ok(JsonValue::String(val.to_string()))
            }
            Type::INT2 | Type::INT4 | Type::INT8 => {
                let val = from_utf8(bytes)?;
                let val: i64 = val.parse()?;
                Ok(JsonValue::Number(val.into()))
            }
            // TODO arrays etc
            ref typ => Err(JsonValueConversionError::UnsupportedType(typ.to_string())),
        }
    }
}
