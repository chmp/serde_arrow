use serde::{Deserialize, Serialize};

use crate::internal::{
    arrow::TimeUnit,
    error::{fail, Error, Result},
    utils::dsl::Term,
};

#[derive(Debug, Clone, PartialEq, PartialOrd, Ord, Eq, Serialize, Deserialize)]
#[serde(try_from = "GenericDataTypeString", into = "GenericDataTypeString")]
pub enum GenericDataType {
    Null,
    Bool,
    I8,
    I16,
    I32,
    I64,
    U8,
    U16,
    U32,
    U64,
    F16,
    F32,
    F64,
    Utf8,
    LargeUtf8,
    Date32,
    Date64,
    Time32(TimeUnit),
    Time64(TimeUnit),
    Duration(TimeUnit),
    Struct,
    List,
    LargeList,
    FixedSizeList(i32),
    Binary,
    LargeBinary,
    FixedSizeBinary(i32),
    Union,
    Map,
    Dictionary,
    Timestamp(TimeUnit, Option<String>),
    Decimal128(u8, i8),
}

impl std::fmt::Display for GenericDataType {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        use GenericDataType::*;
        match self {
            Null => write!(f, "Null"),
            Bool => write!(f, "Bool"),
            Utf8 => write!(f, "Utf8"),
            LargeUtf8 => write!(f, "LargeUtf8"),
            I8 => write!(f, "I8"),
            I16 => write!(f, "I16"),
            I32 => write!(f, "I32"),
            I64 => write!(f, "I64"),
            U8 => write!(f, "U8"),
            U16 => write!(f, "U16"),
            U32 => write!(f, "U32"),
            U64 => write!(f, "U64"),
            F16 => write!(f, "F16"),
            F32 => write!(f, "F32"),
            F64 => write!(f, "F64"),
            Date32 => write!(f, "Date32"),
            Date64 => write!(f, "Date64"),
            Struct => write!(f, "Struct"),
            List => write!(f, "List"),
            LargeList => write!(f, "LargeList"),
            FixedSizeList(n) => write!(f, "FixedSizeList({n})"),
            Binary => write!(f, "Binary"),
            LargeBinary => write!(f, "LargeBinary"),
            FixedSizeBinary(n) => write!(f, "FixedSizeBinary({n})"),
            Union => write!(f, "Union"),
            Map => write!(f, "Map"),
            Dictionary => write!(f, "Dictionary"),
            Timestamp(unit, timezone) => {
                if let Some(timezone) = timezone {
                    write!(f, "Timestamp({unit}, Some(\"{timezone}\"))")
                } else {
                    write!(f, "Timestamp({unit}, None)")
                }
            }
            Time32(unit) => write!(f, "Time32({unit})"),
            Time64(unit) => write!(f, "Time64({unit})"),
            Duration(unit) => write!(f, "Duration({unit})"),
            Decimal128(precision, scale) => write!(f, "Decimal128({precision}, {scale})"),
        }
    }
}

impl std::str::FromStr for GenericDataType {
    type Err = Error;

    fn from_str(s: &str) -> Result<Self> {
        use GenericDataType as T;

        let res = match Term::from_str(s)?.as_call()? {
            ("Null", []) => T::Null,
            ("Bool" | "Boolean", []) => T::Bool,
            ("Utf8", []) => T::Utf8,
            ("LargeUtf8", []) => T::LargeUtf8,
            ("U8" | "UInt8", []) => T::U8,
            ("U16" | "UInt16", []) => T::U16,
            ("U32" | "UInt32", []) => T::U32,
            ("U64" | "UInt64", []) => T::U64,
            ("I8" | "Int8", []) => T::I8,
            ("I16" | "Int16", []) => T::I16,
            ("I32" | "Int32", []) => T::I32,
            ("I64" | "Int64", []) => T::I64,
            ("F16" | "Float16", []) => T::F16,
            ("F32" | "Float32", []) => T::F32,
            ("F64" | "Float64", []) => T::F64,
            ("Date32", []) => T::Date32,
            ("Date64", []) => T::Date64,
            ("Struct", []) => T::Struct,
            ("List", []) => T::List,
            ("LargeList", []) => T::LargeList,
            ("FixedSizeList", [n]) => T::FixedSizeList(n.as_ident()?.parse()?),
            ("Binary", []) => T::Binary,
            ("LargeBinary", []) => T::LargeBinary,
            ("FixedSizeBinary", [n]) => T::FixedSizeBinary(n.as_ident()?.parse()?),
            ("Union", []) => T::Union,
            ("Map", []) => T::Map,
            ("Dictionary", []) => T::Dictionary,
            ("Timestamp", [unit, timezone]) => {
                let unit: TimeUnit = unit.as_ident()?.parse()?;
                let timezone = timezone
                    .as_option()?
                    .map(|term| term.as_string())
                    .transpose()?;
                T::Timestamp(unit, timezone.map(|s| s.to_owned()))
            }
            ("Time32", [unit]) => T::Time32(unit.as_ident()?.parse()?),
            ("Time64", [unit]) => T::Time64(unit.as_ident()?.parse()?),
            ("Duration", [unit]) => T::Duration(unit.as_ident()?.parse()?),
            ("Decimal128", [precision, scale]) => {
                T::Decimal128(precision.as_ident()?.parse()?, scale.as_ident()?.parse()?)
            }
            _ => fail!("invalid data type {s}"),
        };
        Ok(res)
    }
}

#[derive(Serialize, Deserialize)]
struct GenericDataTypeString(String);

impl TryFrom<GenericDataTypeString> for GenericDataType {
    type Error = Error;

    fn try_from(value: GenericDataTypeString) -> std::result::Result<Self, Self::Error> {
        value.0.parse()
    }
}

impl From<GenericDataType> for GenericDataTypeString {
    fn from(value: GenericDataType) -> Self {
        Self(value.to_string())
    }
}
