use arrow2::{
    array::{Array, MutableArray, MutableBooleanArray, MutablePrimitiveArray, MutableUtf8Array},
    chunk::Chunk,
    datatypes::DataType as Arrow2Type,
};
use chrono::{DateTime, NaiveDateTime, Utc};
use serde::Serialize;

use crate::{
    fail,
    ops::{to_arrays, ArrayBuilder},
    DataType, Result, Schema,
};

const DEFAULT_CAPACITY: usize = 1024;

/// Convert a sequence of records into an Arrow Chunk
///
pub fn to_chunk<T>(value: &T, schema: &Schema) -> Result<Chunk<Box<dyn Array + 'static>>>
where
    T: Serialize + ?Sized,
{
    let arrays = to_arrays::<ArrowArrayBuilder, _>(value, schema)?;
    let chunk = Chunk::try_new(arrays)?;
    Ok(chunk)
}

pub enum ArrowArrayBuilder {
    Bool(MutableBooleanArray),
    I8(MutablePrimitiveArray<i8>),
    I16(MutablePrimitiveArray<i16>),
    I32(MutablePrimitiveArray<i32>),
    I64(MutablePrimitiveArray<i64>),
    U8(MutablePrimitiveArray<u8>),
    U16(MutablePrimitiveArray<u16>),
    U32(MutablePrimitiveArray<u32>),
    U64(MutablePrimitiveArray<u64>),
    F32(MutablePrimitiveArray<f32>),
    F64(MutablePrimitiveArray<f64>),
    Utf8(MutableUtf8Array<i32>),
    LargeUtf8(MutableUtf8Array<i64>),
    Date64(MutablePrimitiveArray<i64>),
    Date64Str(MutablePrimitiveArray<i64>),
    Date64NaiveStr(MutablePrimitiveArray<i64>),
}

macro_rules! dispatch {
    ($obj:ident, $builder:pat => $expr:expr) => {
        match $obj {
            Self::Bool($builder) => $expr,
            Self::I8($builder) => $expr,
            Self::I16($builder) => $expr,
            Self::I32($builder) => $expr,
            Self::I64($builder) => $expr,
            Self::U8($builder) => $expr,
            Self::U16($builder) => $expr,
            Self::U32($builder) => $expr,
            Self::U64($builder) => $expr,
            Self::F32($builder) => $expr,
            Self::F64($builder) => $expr,
            Self::Utf8($builder) => $expr,
            Self::LargeUtf8($builder) => $expr,
            Self::Date64($builder) => $expr,
            Self::Date64Str($builder) => $expr,
            Self::Date64NaiveStr($builder) => $expr,
        }
    };
}

macro_rules! simple_append {
    ($name:ident, $ty:ty, $variant:ident) => {
        fn $name(&mut self, value: $ty) -> Result<()> {
            match self {
                Self::$variant(builder) => builder.push(Some(value)),
                _ => fail!("Mismatched type: cannot insert {}", stringify!($ty)),
            };
            Ok(())
        }
    };
}

impl ArrayBuilder for ArrowArrayBuilder {
    type ArrayRef = Box<dyn Array>;

    fn new(data_type: &DataType) -> Result<Self> {
        let res = match data_type {
            DataType::Bool | DataType::Arrow2(Arrow2Type::Boolean) => {
                Self::Bool(MutableBooleanArray::with_capacity(DEFAULT_CAPACITY))
            }
            DataType::I8 | DataType::Arrow2(Arrow2Type::Int8) => {
                Self::I8(MutablePrimitiveArray::with_capacity(DEFAULT_CAPACITY))
            }
            DataType::I16 | DataType::Arrow2(Arrow2Type::Int16) => {
                Self::I16(MutablePrimitiveArray::with_capacity(DEFAULT_CAPACITY))
            }
            DataType::I32 | DataType::Arrow2(Arrow2Type::Int32) => {
                Self::I32(MutablePrimitiveArray::with_capacity(DEFAULT_CAPACITY))
            }
            DataType::I64 | DataType::Arrow2(Arrow2Type::Int64) => {
                Self::I64(MutablePrimitiveArray::with_capacity(DEFAULT_CAPACITY))
            }
            DataType::U8 | DataType::Arrow2(Arrow2Type::UInt8) => {
                Self::U8(MutablePrimitiveArray::with_capacity(DEFAULT_CAPACITY))
            }
            DataType::U16 | DataType::Arrow2(Arrow2Type::UInt16) => {
                Self::U16(MutablePrimitiveArray::with_capacity(DEFAULT_CAPACITY))
            }
            DataType::U32 | DataType::Arrow2(Arrow2Type::UInt32) => {
                Self::U32(MutablePrimitiveArray::with_capacity(DEFAULT_CAPACITY))
            }
            DataType::U64 | DataType::Arrow2(Arrow2Type::UInt64) => {
                Self::U64(MutablePrimitiveArray::with_capacity(DEFAULT_CAPACITY))
            }
            DataType::F32 | DataType::Arrow2(Arrow2Type::Float32) => {
                Self::F32(MutablePrimitiveArray::with_capacity(DEFAULT_CAPACITY))
            }
            DataType::F64 | DataType::Arrow2(Arrow2Type::Float64) => {
                Self::F64(MutablePrimitiveArray::with_capacity(DEFAULT_CAPACITY))
            }
            DataType::Str | DataType::Arrow2(Arrow2Type::Utf8) => {
                Self::Utf8(MutableUtf8Array::with_capacity(DEFAULT_CAPACITY))
            }
            DataType::Arrow2(Arrow2Type::LargeUtf8) => {
                Self::LargeUtf8(MutableUtf8Array::with_capacity(DEFAULT_CAPACITY))
            }
            DataType::DateTimeMilliseconds | DataType::Arrow2(Arrow2Type::Date64) => Self::Date64(
                MutablePrimitiveArray::with_capacity(DEFAULT_CAPACITY).to(Arrow2Type::Date64),
            ),
            DataType::NaiveDateTimeStr => Self::Date64NaiveStr(
                MutablePrimitiveArray::with_capacity(DEFAULT_CAPACITY).to(Arrow2Type::Date64),
            ),
            DataType::DateTimeStr => Self::Date64Str(
                MutablePrimitiveArray::with_capacity(DEFAULT_CAPACITY).to(Arrow2Type::Date64),
            ),
            _ => fail!("Cannot build ArrayBuilder for {:?}", data_type),
        };
        Ok(res)
    }

    fn build(&mut self) -> Result<Self::ArrayRef> {
        let array_ref: Self::ArrayRef = dispatch!(self, builder => builder.as_box());
        Ok(array_ref)
    }

    fn append_null(&mut self) -> Result<()> {
        dispatch!(self, builder => builder.push_null());
        Ok(())
    }

    simple_append!(append_bool, bool, Bool);
    simple_append!(append_i8, i8, I8);
    simple_append!(append_i16, i16, I16);
    simple_append!(append_i32, i32, I32);
    simple_append!(append_u8, u8, U8);
    simple_append!(append_u16, u16, U16);
    simple_append!(append_u32, u32, U32);
    simple_append!(append_u64, u64, U64);
    simple_append!(append_f32, f32, F32);
    simple_append!(append_f64, f64, F64);

    fn append_i64(&mut self, value: i64) -> Result<()> {
        match self {
            Self::I64(builder) => builder.push(Some(value)),
            Self::Date64(builder) => builder.push(Some(value)),
            _ => fail!("Mismatched type: cannot insert {}", stringify!($ty)),
        };
        Ok(())
    }

    fn append_utf8(&mut self, data: &str) -> Result<()> {
        match self {
            Self::Utf8(builder) => builder.push(Some(data)),
            Self::LargeUtf8(builder) => builder.push(Some(data)),
            Self::Date64NaiveStr(builder) => {
                let dt = data.parse::<NaiveDateTime>()?;
                builder.push(Some(dt.timestamp_millis()));
            }
            Self::Date64Str(builder) => {
                let dt = data.parse::<DateTime<Utc>>()?;
                builder.push(Some(dt.timestamp_millis()));
            }
            _ => fail!("Mismatched type"),
        };
        Ok(())
    }
}
