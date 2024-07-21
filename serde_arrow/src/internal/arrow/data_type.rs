use std::{collections::HashMap, sync::Arc};

use serde::{Deserialize, Serialize};

use crate::internal::error::{fail, Error, Result};

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Field {
    pub name: String,
    pub data_type: DataType,
    pub metadata: HashMap<String, String>,
}

#[derive(Debug, Clone, PartialEq, Eq)]
#[non_exhaustive]
pub enum DataType {
    Null,
    Boolean,
    Int8,
    Int16,
    Int32,
    Int64,
    UInt8,
    UInt16,
    UInt32,
    UInt64,
    Float16,
    Float32,
    Float64,
    Utf8,
    LargeUtf8,
    Binary,
    LargeBinary,
    Date32,
    Date64,
    Timestamp(TimeUnit, Option<Arc<str>>),
    Time32(TimeUnit),
    Time64(TimeUnit),
    Decimal128,
    Struct(Vec<Field>),
    List(Box<Field>),
    LargeList(Box<Field>),
}

pub struct BaseDataTypeDisplay<'a>(pub &'a DataType);

impl<'a> std::fmt::Display for BaseDataTypeDisplay<'a> {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self.0 {
            DataType::Null => write!(f, "Null"),
            DataType::Boolean => write!(f, "Boolean"),
            DataType::Int8 => write!(f, "Int8"),
            DataType::Int16 => write!(f, "Int16"),
            DataType::Int32 => write!(f, "Int32"),
            DataType::Int64 => write!(f, "Int64"),
            DataType::UInt8 => write!(f, "UInt8"),
            DataType::UInt16 => write!(f, "UInt16"),
            DataType::UInt32 => write!(f, "UInt32"),
            DataType::UInt64 => write!(f, "UInt64"),
            DataType::Float16 => write!(f, "Float16"),
            DataType::Float32 => write!(f, "Float32"),
            DataType::Float64 => write!(f, "Float64"),
            DataType::Utf8 => write!(f, "Utf8"),
            DataType::LargeUtf8 => write!(f, "LargeUtf8"),
            DataType::Binary => write!(f, "Binary"),
            DataType::LargeBinary => write!(f, "LargeBinary"),
            DataType::Date32 => write!(f, "Date32"),
            DataType::Date64 => write!(f, "Date64"),
            DataType::Timestamp(_, _) => write!(f, "Timestamp"),
            DataType::Time32(_) => write!(f, "Time32"),
            DataType::Time64(_) => write!(f, "Time64"),
            DataType::Decimal128 => write!(f, "Decimal128"),
            DataType::Struct(_) => write!(f, "Struct"),
            DataType::List(_) => write!(f, "List"),
            DataType::LargeList(_) => write!(f, "LargeList"),
        }
    }
}

#[derive(Debug, Clone, Copy, PartialEq, PartialOrd, Ord, Eq, Serialize, Deserialize)]
pub enum TimeUnit {
    Second,
    Millisecond,
    Microsecond,
    Nanosecond,
}

impl std::fmt::Display for TimeUnit {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            TimeUnit::Second => write!(f, "Second"),
            TimeUnit::Millisecond => write!(f, "Millisecond"),
            TimeUnit::Microsecond => write!(f, "Microsecond"),
            TimeUnit::Nanosecond => write!(f, "Nanosecond"),
        }
    }
}

impl std::str::FromStr for TimeUnit {
    type Err = Error;

    fn from_str(s: &str) -> Result<Self> {
        match s {
            "Second" => Ok(Self::Second),
            "Millisecond" => Ok(Self::Millisecond),
            "Microsecond" => Ok(Self::Microsecond),
            "Nanosecond" => Ok(Self::Nanosecond),
            s => fail!("Invalid time unit {s}"),
        }
    }
}
