use std::collections::HashMap;

use serde::{Deserialize, Serialize};

use crate::internal::error::{fail, Error, Result};

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct Field {
    pub name: String,
    pub data_type: DataType,
    pub nullable: bool,
    pub metadata: HashMap<String, String>,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
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
    FixedSizeBinary(i32),
    Date32,
    Date64,
    Timestamp(TimeUnit, Option<String>),
    Time32(TimeUnit),
    Time64(TimeUnit),
    Duration(TimeUnit),
    Decimal128(u8, i8),
    Struct(Vec<Field>),
    List(Box<Field>),
    LargeList(Box<Field>),
    FixedSizeList(Box<Field>, i32),
    Map(Box<Field>, bool),
    Dictionary(Box<DataType>, Box<DataType>, bool),
    Union(Vec<(i8, Field)>, UnionMode),
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
            s => fail!("Invalid TimeUnit: {s}"),
        }
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
pub enum UnionMode {
    Sparse,
    Dense,
}

impl std::fmt::Display for UnionMode {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            UnionMode::Sparse => write!(f, "Sparse"),
            UnionMode::Dense => write!(f, "Dense"),
        }
    }
}

impl std::str::FromStr for UnionMode {
    type Err = Error;

    fn from_str(s: &str) -> std::result::Result<Self, Self::Err> {
        match s {
            "Sparse" => Ok(UnionMode::Sparse),
            "Dense" => Ok(UnionMode::Dense),
            s => fail!("Invalid UnionMode: {s}"),
        }
    }
}
