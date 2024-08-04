//! Owned versions of the different array types
use std::collections::HashMap;

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
    Utf8(BytesArray<i32>),
    LargeUtf8(BytesArray<i64>),
    Binary(BytesArray<i32>),
    LargeBinary(BytesArray<i64>),
    FixedSizeBinary(FixedSizeBinaryArray),
    Decimal128(DecimalArray<i128>),
    Struct(StructArray),
    List(ListArray<i32>),
    LargeList(ListArray<i64>),
    FixedSizeList(FixedSizeListArray),
    Dictionary(DictionaryArray),
    Map(ListArray<i32>),
    DenseUnion(DenseUnionArray),
}

#[derive(Clone, Debug)]
#[non_exhaustive]
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
    Timestamp(TimestampArrayView<'a>),
    Duration(TimeArrayView<'a, i64>),
    Utf8(BytesArrayView<'a, i32>),
    LargeUtf8(BytesArrayView<'a, i64>),
    Binary(BytesArrayView<'a, i32>),
    LargeBinary(BytesArrayView<'a, i64>),
    FixedSizeBinary(FixedSizeBinaryArrayView<'a>),
    Decimal128(DecimalArrayView<'a, i128>),
    Struct(StructArrayView<'a>),
    List(ListArrayView<'a, i32>),
    LargeList(ListArrayView<'a, i64>),
    FixedSizeList(FixedSizeListArrayView<'a>),
    Dictionary(DictionaryArrayView<'a>),
    Map(ListArrayView<'a, i32>),
    DenseUnion(DenseUnionArrayView<'a>),
}

#[derive(Debug, Clone, Copy)]
pub struct BitsWithOffset<'a> {
    pub offset: usize,
    pub data: &'a [u8],
}

#[derive(Clone, Debug)]
pub struct FieldMeta {
    pub name: String,
    pub nullable: bool,
    pub metadata: HashMap<String, String>,
}

#[derive(Clone, Debug)]
pub struct NullArray {
    pub len: usize,
}

#[derive(Clone, Debug)]
pub struct NullArrayView {
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
pub struct BooleanArrayView<'a> {
    pub len: usize,
    pub validity: Option<BitsWithOffset<'a>>,
    pub values: BitsWithOffset<'a>,
}

#[derive(Clone, Debug)]
pub struct PrimitiveArray<T> {
    pub validity: Option<Vec<u8>>,
    pub values: Vec<T>,
}

#[derive(Clone, Debug)]
pub struct PrimitiveArrayView<'a, T> {
    pub validity: Option<BitsWithOffset<'a>>,
    pub values: &'a [T],
}

#[derive(Debug, Clone)]
pub struct TimeArray<T> {
    pub unit: TimeUnit,
    pub validity: Option<Vec<u8>>,
    pub values: Vec<T>,
}

#[derive(Debug, Clone)]
pub struct TimeArrayView<'a, T> {
    pub unit: TimeUnit,
    pub validity: Option<BitsWithOffset<'a>>,
    pub values: &'a [T],
}

#[derive(Debug, Clone)]

pub struct TimestampArray {
    pub unit: TimeUnit,
    pub timezone: Option<String>,
    pub validity: Option<Vec<u8>>,
    pub values: Vec<i64>,
}

#[derive(Debug, Clone)]

pub struct TimestampArrayView<'a> {
    pub unit: TimeUnit,
    pub timezone: Option<String>,
    pub validity: Option<BitsWithOffset<'a>>,
    pub values: &'a [i64],
}

#[derive(Clone, Debug)]
pub struct StructArray {
    pub len: usize,
    pub validity: Option<Vec<u8>>,
    pub fields: Vec<(Array, FieldMeta)>,
}

#[derive(Clone, Debug)]
pub struct StructArrayView<'a> {
    pub len: usize,
    pub validity: Option<BitsWithOffset<'a>>,
    pub fields: Vec<(ArrayView<'a>, FieldMeta)>,
}

#[derive(Clone, Debug)]
pub struct ListArray<O> {
    pub validity: Option<Vec<u8>>,
    pub offsets: Vec<O>,
    pub meta: FieldMeta,
    pub element: Box<Array>,
}

#[derive(Clone, Debug)]
pub struct ListArrayView<'a, O> {
    pub validity: Option<BitsWithOffset<'a>>,
    pub offsets: &'a [O],
    pub meta: FieldMeta,
    pub element: Box<ArrayView<'a>>,
}

/// An array comprised of lists of fixed size
#[derive(Clone, Debug)]
pub struct FixedSizeListArray {
    /// The number of elements in this array, each a list with `n` children
    pub len: usize,
    /// The number of children per element
    pub n: i32,
    /// The validity mask of the elements
    pub validity: Option<Vec<u8>>,
    pub meta: FieldMeta,
    pub element: Box<Array>,
}

#[derive(Clone, Debug)]
pub struct FixedSizeListArrayView<'a> {
    pub len: usize,
    pub n: i32,
    pub validity: Option<BitsWithOffset<'a>>,
    pub meta: FieldMeta,
    pub element: Box<ArrayView<'a>>,
}

#[derive(Clone, Debug)]
pub struct BytesArray<O> {
    pub validity: Option<Vec<u8>>,
    pub offsets: Vec<O>,
    pub data: Vec<u8>,
}

#[derive(Clone, Debug)]
pub struct BytesArrayView<'a, O> {
    pub validity: Option<BitsWithOffset<'a>>,
    pub offsets: &'a [O],
    pub data: &'a [u8],
}

#[derive(Clone, Debug)]
pub struct FixedSizeBinaryArray {
    pub n: i32,
    pub validity: Option<Vec<u8>>,
    pub data: Vec<u8>,
}

#[derive(Clone, Debug)]
pub struct FixedSizeBinaryArrayView<'a> {
    pub n: i32,
    pub validity: Option<BitsWithOffset<'a>>,
    pub data: &'a [u8],
}

#[derive(Clone, Debug)]
pub struct DecimalArray<T> {
    pub precision: u8,
    pub scale: i8,
    pub validity: Option<Vec<u8>>,
    pub values: Vec<T>,
}

#[derive(Clone, Debug)]
pub struct DecimalArrayView<'a, T> {
    pub precision: u8,
    pub scale: i8,
    pub validity: Option<BitsWithOffset<'a>>,
    pub values: &'a [T],
}

#[derive(Clone, Debug)]
pub struct DictionaryArray {
    pub indices: Box<Array>,
    pub values: Box<Array>,
}

#[derive(Clone, Debug)]
pub struct DictionaryArrayView<'a> {
    pub indices: Box<ArrayView<'a>>,
    pub values: Box<ArrayView<'a>>,
}

#[derive(Clone, Debug)]
pub struct DenseUnionArray {
    pub types: Vec<i8>,
    pub offsets: Vec<i32>,
    pub fields: Vec<(Array, FieldMeta)>,
}

#[derive(Clone, Debug)]
pub struct DenseUnionArrayView<'a> {
    pub types: &'a [i8],
    pub offsets: &'a [i32],
    pub fields: Vec<(ArrayView<'a>, FieldMeta)>,
}
