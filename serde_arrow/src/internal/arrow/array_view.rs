#![allow(dead_code, unused)]
use half::f16;

use crate::internal::arrow::data_type::TimeUnit;

pub enum ArrayView<'a> {
    Null(NullArrayView),
    Boolean(BooleanArrayView<'a>),
    Int8(PrimitiveArrayView<'a, i8>),
    Int16(PrimitiveArrayView<'a, i16>),
    Int32(PrimitiveArrayView<'a, i32>),
    Int64(PrimitiveArrayView<'a, i64>),
    UInt8(PrimitiveArrayView<'a, u8>),
    UInt16(PrimitiveArrayView<'a, u16>),
    UInt32(PrimitiveArrayView<'a, u32>),
    UInt64(PrimitiveArrayView<'a, u64>),
    Float16(PrimitiveArrayView<'a, f16>),
    Float32(PrimitiveArrayView<'a, f32>),
    Float64(PrimitiveArrayView<'a, f64>),
    Date32(PrimitiveArrayView<'a, i32>),
    Date64(PrimitiveArrayView<'a, i64>),
    Time32(TimeArrayView<'a, i32>),
    Time64(TimeArrayView<'a, i64>),
    Utf8(Utf8ArrayView<'a, i32>),
    LargeUtf8(Utf8ArrayView<'a, i64>),
    Binary(Utf8ArrayView<'a, i32>),
    LargeBinary(Utf8ArrayView<'a, i64>),
    Decimal128(DecimalArrayView<'a, i128>),
    Struct(StructArrayView<'a>),
    List(ListArrayView<'a, i32>),
    LargeList(ListArrayView<'a, i64>),
}

pub struct NullArrayView {
    pub len: usize,
}

#[derive(Debug, Clone, Copy)]
pub struct BitsWithOffset<'a> {
    pub offset: usize,
    pub data: &'a [u8],
}

pub struct BooleanArrayView<'a> {
    pub len: usize,
    pub validity: Option<BitsWithOffset<'a>>,
    pub values: BitsWithOffset<'a>,
}

pub struct PrimitiveArrayView<'a, T> {
    pub validity: Option<BitsWithOffset<'a>>,
    pub values: &'a [T],
}

pub struct DecimalArrayView<'a, T> {
    pub precision: u8,
    pub scale: i8,
    pub validity: Option<BitsWithOffset<'a>>,
    pub values: &'a [T],
}

pub struct TimeArrayView<'a, T> {
    pub unit: TimeUnit,
    pub validity: Option<BitsWithOffset<'a>>,
    pub values: &'a [T],
}

pub struct StructArrayView<'a> {
    pub len: usize,
    pub validity: Option<BitsWithOffset<'a>>,
    pub fields: Vec<ArrayView<'a>>,
}

pub struct ListArrayView<'a, O> {
    pub len: usize,
    pub validity: Option<&'a [u8]>,
    pub offsets: &'a [O],
    pub element: Box<ArrayView<'a>>,
}

pub struct Utf8ArrayView<'a, O> {
    pub len: usize,
    pub validity: Option<&'a [u8]>,
    pub offsets: &'a [O],
    pub data: &'a [u8],
}
