//! Owned versions of the different array types
use half::f16;

use crate::internal::arrow::{
    array_view::{
        ArrayView, BooleanArrayView, ListArrayView, NullArrayView, PrimitiveArrayView,
        StructArrayView, Utf8ArrayView,
    },
    data_type::TimeUnit,
};

use super::array_view::TimeArrayView;

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
    Utf8(Utf8Array<i32>),
    LargeUtf8(Utf8Array<i64>),
    Binary(Utf8Array<i32>),
    LargeBinary(Utf8Array<i64>),
    Decimal128(PrimitiveArray<i128>),
    Struct(StructArray),
    List(ListArray<i32>),
    LargeList(ListArray<i64>),
}

impl Array {
    pub fn view(&self) -> ArrayView {
        match self {
            Self::Null(array) => ArrayView::Null(array.view()),
            Self::Boolean(array) => ArrayView::Boolean(array.view()),
            Self::Int8(array) => ArrayView::Int8(array.view()),
            Self::Int16(array) => ArrayView::Int16(array.view()),
            Self::Int32(array) => ArrayView::Int32(array.view()),
            Self::Int64(array) => ArrayView::Int64(array.view()),
            Self::UInt8(array) => ArrayView::UInt8(array.view()),
            Self::UInt16(array) => ArrayView::UInt16(array.view()),
            Self::UInt32(array) => ArrayView::UInt32(array.view()),
            Self::UInt64(array) => ArrayView::UInt64(array.view()),
            Self::Float16(array) => ArrayView::Float16(array.view()),
            Self::Float32(array) => ArrayView::Float32(array.view()),
            Self::Float64(array) => ArrayView::Float64(array.view()),
            Self::Date32(array) => ArrayView::Date32(array.view()),
            Self::Date64(array) => ArrayView::Date64(array.view()),
            Self::Time32(array) => ArrayView::Time32(array.view()),
            Self::Time64(array) => ArrayView::Time64(array.view()),
            Self::Utf8(array) => ArrayView::Utf8(array.view()),
            Self::LargeUtf8(array) => ArrayView::LargeUtf8(array.view()),
            Self::Binary(array) => ArrayView::Binary(array.view()),
            Self::LargeBinary(array) => ArrayView::LargeBinary(array.view()),
            Self::Decimal128(array) => ArrayView::Decimal128(array.view()),
            Self::Struct(array) => ArrayView::Struct(array.view()),
            Self::List(array) => ArrayView::List(array.view()),
            Self::LargeList(array) => ArrayView::LargeList(array.view()),
        }
    }
}

#[derive(Clone, Debug)]
pub struct NullArray {
    pub len: usize,
}

impl NullArray {
    pub fn view(&self) -> NullArrayView {
        NullArrayView { len: self.len }
    }
}

#[derive(Clone, Debug)]
pub struct BooleanArray {
    pub len: usize,
    pub validity: Option<Vec<u8>>,
    pub values: Vec<u8>,
}

impl BooleanArray {
    pub fn view(&self) -> BooleanArrayView {
        BooleanArrayView {
            len: self.len,
            validity: self.validity.as_deref(),
            values: &self.values,
        }
    }
}

#[derive(Clone, Debug)]
pub struct PrimitiveArray<T> {
    pub validity: Option<Vec<u8>>,
    pub values: Vec<T>,
}

impl<T> PrimitiveArray<T> {
    pub fn view(&self) -> PrimitiveArrayView<T> {
        PrimitiveArrayView {
            validity: self.validity.as_deref(),
            values: &self.values,
        }
    }

    pub fn map_values<R>(self, func: impl Fn(T) -> R) -> PrimitiveArray<R> {
        PrimitiveArray {
            validity: self.validity,
            values: self.values.into_iter().map(func).collect(),
        }
    }
}

#[derive(Debug, Clone)]
pub struct TimeArray<T> {
    pub unit: TimeUnit,
    pub validity: Option<Vec<u8>>,
    pub values: Vec<T>,
}

impl<T> TimeArray<T> {
    pub fn view(&self) -> TimeArrayView<T> {
        TimeArrayView {
            unit: self.unit,
            validity: self.validity.as_deref(),
            values: &self.values,
        }
    }
}

#[derive(Clone, Debug)]
pub struct StructArray {
    pub len: usize,
    pub validity: Option<Vec<u8>>,
    pub fields: Vec<Array>,
}

impl StructArray {
    pub fn view(&self) -> StructArrayView {
        StructArrayView {
            len: self.len,
            validity: self.validity.as_deref(),
            fields: self.fields.iter().map(|f| f.view()).collect(),
        }
    }
}

#[derive(Clone, Debug)]
pub struct ListArray<O> {
    pub len: usize,
    pub validity: Option<Vec<u8>>,
    pub offsets: Vec<O>,
    pub element: Box<Array>,
}

impl<O> ListArray<O> {
    pub fn view(&self) -> ListArrayView<O> {
        ListArrayView {
            len: self.len,
            validity: self.validity.as_deref(),
            offsets: &self.offsets,
            element: Box::new(self.element.view()),
        }
    }
}

#[derive(Clone, Debug)]
pub struct Utf8Array<O> {
    pub len: usize,
    pub validity: Option<Vec<u8>>,
    pub offsets: Vec<O>,
    pub data: Vec<u8>,
}

impl<O> Utf8Array<O> {
    pub fn view(&self) -> Utf8ArrayView<O> {
        Utf8ArrayView {
            len: self.len,
            validity: self.validity.as_deref(),
            offsets: &self.offsets,
            data: &self.data,
        }
    }
}
