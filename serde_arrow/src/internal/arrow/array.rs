//! Owned versions of the different array types
use half::f16;

use crate::internal::arrow::data_type::TimeUnit;

#[derive(Clone, Debug)]
#[non_exhaustive]
pub enum Array {
    Null(NullArray),
    Boolean(BooleanArray),
    Int8(PrimitiveArray<i8>),
    Int16(PrimitiveArray<i16>),
    Int32(PrimitiveArray<i32>),
    Int64(PrimitiveArray<i64>),
    UInt8(PrimitiveArray<u8>),
    UInt16(PrimitiveArray<u16>),
    UInt32(PrimitiveArray<u32>),
    UInt64(PrimitiveArray<u64>),
    Float16(PrimitiveArray<f16>),
    Float32(PrimitiveArray<f32>),
    Float64(PrimitiveArray<f64>),
    Date32(PrimitiveArray<i32>),
    Date64(PrimitiveArray<i64>),
    Time32(TimeArray<i32>),
    Time64(TimeArray<i64>),
    Timestamp(TimestampArray),
    Duration(TimeArray<i64>),
    Utf8(Utf8Array<i32>),
    LargeUtf8(Utf8Array<i64>),
    Binary(Utf8Array<i32>),
    LargeBinary(Utf8Array<i64>),
    Decimal128(DecimalArray<i128>),
    Struct(StructArray),
    List(ListArray<i32>),
    LargeList(ListArray<i64>),
}

#[derive(Clone, Debug)]
pub struct NullArray {
    pub len: usize,
}

#[derive(Clone, Debug)]
pub struct BooleanArray {
    // Note: len is required to know how many bits of values are used
    pub len: usize,
    pub validity: Option<Vec<u8>>,
    pub values: Vec<u8>,
}

#[derive(Clone, Debug)]
pub struct PrimitiveArray<T> {
    pub validity: Option<Vec<u8>>,
    pub values: Vec<T>,
}

#[derive(Debug, Clone)]
pub struct TimeArray<T> {
    pub unit: TimeUnit,
    pub validity: Option<Vec<u8>>,
    pub values: Vec<T>,
}

#[derive(Debug, Clone)]

pub struct TimestampArray {
    pub unit: TimeUnit,
    pub timezone: Option<String>,
    pub validity: Option<Vec<u8>>,
    pub values: Vec<i64>,
}

#[derive(Clone, Debug)]
pub struct StructArray {
    pub len: usize,
    pub validity: Option<Vec<u8>>,
    pub fields: Vec<Array>,
}

#[derive(Clone, Debug)]
pub struct ListArray<O> {
    pub len: usize,
    pub validity: Option<Vec<u8>>,
    pub offsets: Vec<O>,
    pub element: Box<Array>,
}

#[derive(Clone, Debug)]
pub struct Utf8Array<O> {
    pub len: usize,
    pub validity: Option<Vec<u8>>,
    pub offsets: Vec<O>,
    pub data: Vec<u8>,
}

#[derive(Clone, Debug)]
pub struct DecimalArray<T> {
    pub precision: u8,
    pub scale: i8,
    pub validity: Option<Vec<u8>>,
    pub values: Vec<T>,
}
