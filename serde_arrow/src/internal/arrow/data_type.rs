use std::{collections::HashMap, sync::Arc};

use serde::{Deserialize, Serialize};

use crate::internal::error::{fail, Error, Result};

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Field {
    pub name: String,
    pub data_type: DataType,
    pub metadata: HashMap<String, String>,
}

#[allow(unused)]
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
