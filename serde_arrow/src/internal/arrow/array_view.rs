use half::f16;

use crate::internal::arrow::{
    array::{
        Array, BooleanArray, ListArray, NullArray, PrimitiveArray, StructArray, TimeArray,
        Utf8Array,
    },
    data_type::TimeUnit,
};

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
    Decimal128(PrimitiveArrayView<'a, i128>),
    Struct(StructArrayView<'a>),
    List(ListArrayView<'a, i32>),
    LargeList(ListArrayView<'a, i64>),
}

impl<'a> ArrayView<'a> {
    pub fn array(&self) -> Array {
        match self {
            Self::Null(view) => Array::Null(view.array()),
            Self::Boolean(view) => Array::Boolean(view.array()),
            Self::Int8(view) => Array::Int8(view.array()),
            Self::Int16(view) => Array::Int16(view.array()),
            Self::Int32(view) => Array::Int32(view.array()),
            Self::Int64(view) => Array::Int64(view.array()),
            Self::UInt8(view) => Array::UInt8(view.array()),
            Self::UInt16(view) => Array::UInt16(view.array()),
            Self::UInt32(view) => Array::UInt32(view.array()),
            Self::UInt64(view) => Array::UInt64(view.array()),
            Self::Float16(view) => Array::Float16(view.array()),
            Self::Float32(view) => Array::Float32(view.array()),
            Self::Float64(view) => Array::Float64(view.array()),
            Self::Date32(view) => Array::Date32(view.array()),
            Self::Date64(view) => Array::Date64(view.array()),
            Self::Time32(view) => Array::Time32(view.array()),
            Self::Time64(view) => Array::Time64(view.array()),
            Self::Utf8(view) => Array::Utf8(view.array()),
            Self::Binary(view) => Array::Binary(view.array()),
            Self::LargeBinary(view) => Array::LargeBinary(view.array()),
            Self::LargeUtf8(view) => Array::LargeUtf8(view.array()),
            Self::Decimal128(view) => Array::Decimal128(view.array()),
            Self::Struct(view) => Array::Struct(view.array()),
            Self::List(view) => Array::List(view.array()),
            Self::LargeList(view) => Array::LargeList(view.array()),
        }
    }
}

pub struct NullArrayView {
    pub len: usize,
}

impl NullArrayView {
    pub fn array(&self) -> NullArray {
        NullArray { len: self.len }
    }
}

pub struct BooleanArrayView<'a> {
    pub len: usize,
    pub validity: Option<&'a [u8]>,
    pub values: &'a [u8],
}

impl<'a> BooleanArrayView<'a> {
    pub fn array(&self) -> BooleanArray {
        BooleanArray {
            len: self.len,
            validity: self.validity.map(<[_]>::to_vec),
            values: self.values.to_owned(),
        }
    }
}

pub struct PrimitiveArrayView<'a, T> {
    pub validity: Option<&'a [u8]>,
    pub values: &'a [T],
}

impl<'a, T: Clone> PrimitiveArrayView<'a, T> {
    pub fn array(&self) -> PrimitiveArray<T> {
        PrimitiveArray {
            validity: self.validity.map(<[_]>::to_vec),
            values: self.values.to_vec(),
        }
    }
}

pub struct TimeArrayView<'a, T> {
    pub unit: TimeUnit,
    pub validity: Option<&'a [u8]>,
    pub values: &'a [T],
}

impl<'a, T: Clone> TimeArrayView<'a, T> {
    pub fn array(&self) -> TimeArray<T> {
        TimeArray {
            unit: self.unit,
            validity: self.validity.map(<[_]>::to_vec),
            values: self.values.to_vec(),
        }
    }
}

pub struct StructArrayView<'a> {
    pub len: usize,
    pub validity: Option<&'a [u8]>,
    pub fields: Vec<ArrayView<'a>>,
}

impl<'a> StructArrayView<'a> {
    pub fn array(&self) -> StructArray {
        StructArray {
            len: self.len,
            validity: self.validity.map(<[_]>::to_vec),
            fields: self.fields.iter().map(|f| f.array()).collect(),
        }
    }
}

pub struct ListArrayView<'a, O> {
    pub len: usize,
    pub validity: Option<&'a [u8]>,
    pub offsets: &'a [O],
    pub element: Box<ArrayView<'a>>,
}

impl<'a, O: Clone> ListArrayView<'a, O> {
    pub fn array(&self) -> ListArray<O> {
        ListArray {
            len: self.len,
            validity: self.validity.map(<[_]>::to_vec),
            offsets: self.offsets.to_vec(),
            element: Box::new(self.element.array()),
        }
    }
}

pub struct Utf8ArrayView<'a, O> {
    pub len: usize,
    pub validity: Option<&'a [u8]>,
    pub offsets: &'a [O],
    pub data: &'a [u8],
}

impl<'a, O: Clone> Utf8ArrayView<'a, O> {
    pub fn array(&self) -> Utf8Array<O> {
        Utf8Array {
            len: self.len,
            validity: self.validity.map(<[_]>::to_vec),
            offsets: self.offsets.to_vec(),
            data: self.data.to_vec(),
        }
    }
}
