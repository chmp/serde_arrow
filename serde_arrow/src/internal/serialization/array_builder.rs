use half::f16;
use serde::Serialize;

use crate::internal::error::Result;

use super::{
    bool_builder::BoolBuilder, date32_builder::Date32Builder, date64_builder::Date64Builder,
    decimal_builder::DecimalBuilder, dictionary_utf8_builder::DictionaryUtf8Builder,
    float_builder::FloatBuilder, int_builder::IntBuilder, list_builder::ListBuilder,
    map_builder::MapBuilder, null_builder::NullBuilder, struct_builder::StructBuilder,
    time64_builder::Time64Builder, union_builder::UnionBuilder,
    unknown_variant_builder::UnknownVariantBuilder, utf8_builder::Utf8Builder,
    utils::SimpleSerializer,
};

// TODO: add outer sequence builder? (not limited by i64 limits)
#[derive(Debug, Clone)]
pub enum ArrayBuilder {
    Null(NullBuilder),
    Bool(BoolBuilder),
    I8(IntBuilder<i8>),
    I16(IntBuilder<i16>),
    I32(IntBuilder<i32>),
    I64(IntBuilder<i64>),
    U8(IntBuilder<u8>),
    U16(IntBuilder<u16>),
    U32(IntBuilder<u32>),
    U64(IntBuilder<u64>),
    F16(FloatBuilder<f16>),
    F32(FloatBuilder<f32>),
    F64(FloatBuilder<f64>),
    Date32(Date32Builder),
    Date64(Date64Builder),
    Time64(Time64Builder),
    Decimal128(DecimalBuilder),
    List(ListBuilder<i32>),
    LargeList(ListBuilder<i64>),
    Map(MapBuilder),
    Struct(StructBuilder),
    Utf8(Utf8Builder<i32>),
    LargeUtf8(Utf8Builder<i64>),
    DictionaryUtf8(DictionaryUtf8Builder),
    Union(UnionBuilder),
    UnknownVariant(UnknownVariantBuilder),
}

macro_rules! dispatch {
    ($obj:expr, $wrapper:ident($name:ident) => $expr:expr) => {
        match $obj {
            $wrapper::Bool($name) => $expr,
            $wrapper::Null($name) => $expr,
            $wrapper::I8($name) => $expr,
            $wrapper::I16($name) => $expr,
            $wrapper::I32($name) => $expr,
            $wrapper::I64($name) => $expr,
            $wrapper::U8($name) => $expr,
            $wrapper::U16($name) => $expr,
            $wrapper::U32($name) => $expr,
            $wrapper::U64($name) => $expr,
            $wrapper::F16($name) => $expr,
            $wrapper::F32($name) => $expr,
            $wrapper::F64($name) => $expr,
            $wrapper::Date32($name) => $expr,
            $wrapper::Date64($name) => $expr,
            $wrapper::Time64($name) => $expr,
            $wrapper::Decimal128($name) => $expr,
            $wrapper::Utf8($name) => $expr,
            $wrapper::LargeUtf8($name) => $expr,
            $wrapper::List($name) => $expr,
            $wrapper::LargeList($name) => $expr,
            $wrapper::Map($name) => $expr,
            $wrapper::Struct($name) => $expr,
            $wrapper::DictionaryUtf8($name) => $expr,
            $wrapper::Union($name) => $expr,
            $wrapper::UnknownVariant($name) => $expr,
        }
    };
}

impl ArrayBuilder {
    pub fn name(&self) -> &'static str {
        match self {
            Self::Null(_) => "Null",
            Self::Bool(_) => "Bool",
            Self::I8(_) => "I8",
            Self::I16(_) => "I16",
            Self::I32(_) => "I32",
            Self::I64(_) => "I64",
            Self::U8(_) => "U8",
            Self::U16(_) => "U16",
            Self::U32(_) => "U32",
            Self::U64(_) => "U64",
            Self::F16(_) => "F16",
            Self::F32(_) => "F32",
            Self::F64(_) => "F64",
            Self::Date32(_) => "Date32",
            Self::Date64(_) => "Date64",
            Self::Time64(_) => "Time64",
            Self::Decimal128(_) => "Decimal128",
            Self::Utf8(_) => "Utf8",
            Self::LargeUtf8(_) => "LargeUtf8",
            Self::List(_) => "List",
            Self::LargeList(_) => "LargeList",
            Self::Struct(_) => "Struct",
            Self::Map(_) => "Map",
            Self::DictionaryUtf8(_) => "DictionaryUtf8",
            Self::Union(_) => "Union",
            Self::UnknownVariant(_) => "UnknownVariant",
        }
    }

    pub fn is_nullable(&self) -> bool {
        dispatch!(self, Self(builder) => builder.is_nullable())
    }
}

impl ArrayBuilder {
    /// Take the contained array builder, while leaving structure intact
    pub fn take(&mut self) -> ArrayBuilder {
        match self {
            Self::Null(builder) => Self::Null(builder.take()),
            Self::Bool(builder) => Self::Bool(builder.take()),
            Self::I8(builder) => Self::I8(builder.take()),
            Self::I16(builder) => Self::I16(builder.take()),
            Self::I32(builder) => Self::I32(builder.take()),
            Self::I64(builder) => Self::I64(builder.take()),
            Self::U8(builder) => Self::U8(builder.take()),
            Self::U16(builder) => Self::U16(builder.take()),
            Self::U32(builder) => Self::U32(builder.take()),
            Self::U64(builder) => Self::U64(builder.take()),
            Self::F16(builder) => Self::F16(builder.take()),
            Self::F32(builder) => Self::F32(builder.take()),
            Self::F64(builder) => Self::F64(builder.take()),
            Self::Date32(builder) => Self::Date32(builder.take()),
            Self::Date64(builder) => Self::Date64(builder.take()),
            Self::Time64(builder) => Self::Time64(builder.take()),
            Self::Decimal128(builder) => Self::Decimal128(builder.take()),
            Self::Utf8(builder) => Self::Utf8(builder.take()),
            Self::LargeUtf8(builder) => Self::LargeUtf8(builder.take()),
            Self::List(builder) => Self::List(builder.take()),
            Self::LargeList(builder) => Self::LargeList(builder.take()),
            Self::Struct(builder) => Self::Struct(builder.take()),
            Self::Map(builder) => Self::Map(builder.take()),
            Self::DictionaryUtf8(builder) => Self::DictionaryUtf8(builder.take()),
            Self::Union(builder) => Self::Union(builder.take()),
            Self::UnknownVariant(builder) => Self::UnknownVariant(builder.take()),
        }
    }

    pub fn reserve(&mut self, num_elements: usize) -> Result<()> {
        dispatch!(self, Self(builder) => builder.reserve(num_elements))
    }
}

#[rustfmt::skip]
impl SimpleSerializer for ArrayBuilder {
    fn name(&self) -> &str {
        "ArrayBuilder"
    }

    fn serialize_default(&mut self) -> Result<()> {
        dispatch!(self, Self(builder) => builder.serialize_default())
    }

    fn serialize_unit_struct(&mut self, name: &'static str) -> Result<()> {
        dispatch!(self, Self(builder) => builder.serialize_unit_struct(name))
    }

    fn serialize_none(&mut self) -> Result<()> {
        dispatch!(self, Self(builder) => builder.serialize_none())
    }

    fn serialize_some<V: Serialize + ?Sized>(&mut self, value: &V) -> Result<()> {
        dispatch!(self, Self(builder) => builder.serialize_some(value))
    }

    fn serialize_unit(&mut self) -> Result<()> {
        dispatch!(self, Self(builder) => builder.serialize_unit())
    }

    fn serialize_bool(&mut self, v: bool) -> Result<()> {
        dispatch!(self, Self(builder) => builder.serialize_bool(v))
    }

    fn serialize_i8(&mut self, v: i8) -> Result<()> {
        dispatch!(self, Self(builder) => builder.serialize_i8(v))
    }

    fn serialize_i16(&mut self, v: i16) -> Result<()> {
        dispatch!(self, Self(builder) => builder.serialize_i16(v))
    }

    fn serialize_i32(&mut self, v: i32) -> Result<()> {
        dispatch!(self, Self(builder) => builder.serialize_i32(v))
    }

    fn serialize_i64(&mut self, v: i64) -> Result<()> {
        dispatch!(self, Self(builder) => builder.serialize_i64(v))
    }

    fn serialize_u8(&mut self, v: u8) -> Result<()> {
        dispatch!(self, Self(builder) => builder.serialize_u8(v))
    }

    fn serialize_u16(&mut self, v: u16) -> Result<()> {
        dispatch!(self, Self(builder) => builder.serialize_u16(v))
    }

    fn serialize_u32(&mut self, v: u32) -> Result<()> {
        dispatch!(self, Self(builder) => builder.serialize_u32(v))
    }

    fn serialize_u64(&mut self, v: u64) -> Result<()> {
        dispatch!(self, Self(builder) => builder.serialize_u64(v))
    }

    fn serialize_f32(&mut self, v: f32) -> Result<()> {
        dispatch!(self, Self(builder) => builder.serialize_f32(v))
    }

    fn serialize_f64(&mut self, v: f64) -> Result<()> {
        dispatch!(self, Self(builder) => builder.serialize_f64(v))
    }

    fn serialize_char(&mut self, v: char) -> Result<()> {
        dispatch!(self, Self(builder) => builder.serialize_char(v))
    }

    fn serialize_str(&mut self, v: &str) -> Result<()> {
        dispatch!(self, Self(builder) => builder.serialize_str(v))
    }

    fn serialize_bytes(&mut self, v: &[u8]) -> Result<()> {
        dispatch!(self, Self(builder) => builder.serialize_bytes(v))
    }

    fn serialize_seq_start(&mut self, len: Option<usize>) -> Result<()> {
        dispatch!(self, Self(builder) => builder.serialize_seq_start(len))
    }

    fn serialize_seq_element<V: Serialize + ?Sized>(&mut self, value: &V) -> Result<()> {
        dispatch!(self, Self(builder) => builder.serialize_seq_element(value))
    }

    fn serialize_seq_end(&mut self) -> Result<()> {
        dispatch!(self, Self(builder) => builder.serialize_seq_end())
    }

    fn serialize_struct_start(&mut self, name: &'static str, len: usize) -> Result<()> {
        dispatch!(self, Self(builder) => builder.serialize_struct_start(name, len))
    }

    fn serialize_struct_field<V: Serialize + ?Sized>(&mut self, key: &'static str, value: &V) -> Result<()> {
        dispatch!(self, Self(builder) => builder.serialize_struct_field(key, value))
    }

    fn serialize_struct_end(&mut self) -> Result<()> {
        dispatch!(self, Self(builder) => builder.serialize_struct_end())
    }

    fn serialize_map_start(&mut self, len: Option<usize>) -> Result<()> {
        dispatch!(self, Self(builder) => builder.serialize_map_start(len))
    }

    fn serialize_map_key<V: Serialize + ?Sized>(&mut self, key: &V) -> Result<()> {
        dispatch!(self, Self(builder) => builder.serialize_map_key(key))
    }

    fn serialize_map_value<V: Serialize + ?Sized>(&mut self, value: &V) -> Result<()> {
        dispatch!(self, Self(builder) => builder.serialize_map_value(value))
    }

    fn serialize_map_end(&mut self) -> Result<()> {
        dispatch!(self, Self(builder) => builder.serialize_map_end())
    }

    fn serialize_tuple_start(&mut self, len: usize) -> Result<()> {
        dispatch!(self, Self(builder) => builder.serialize_tuple_start(len))
    }

    fn serialize_tuple_element<V: Serialize + ?Sized>(&mut self, value: &V) -> Result<()> {
        dispatch!(self, Self(builder) => builder.serialize_tuple_element(value))
    }

    fn serialize_tuple_end(&mut self) -> Result<()> {
        dispatch!(self, Self(builder) => builder.serialize_tuple_end())
    }

    fn serialize_tuple_struct_start(&mut self, name: &'static str, len: usize) -> Result<()> {
        dispatch!(self, Self(builder) => builder.serialize_tuple_struct_start(name, len))
    }

    fn serialize_tuple_struct_field<V: Serialize + ?Sized>(&mut self, value: &V) -> Result<()> {
        dispatch!(self, Self(builder) => builder.serialize_tuple_struct_field(value))
    }

    fn serialize_tuple_struct_end(&mut self) -> Result<()> {
        dispatch!(self, Self(builder) => builder.serialize_tuple_struct_end())
    }

    fn serialize_newtype_struct<V: Serialize + ?Sized>(&mut self, name: &'static str, value: &V) -> Result<()> {
        dispatch!(self, Self(builder) => builder.serialize_newtype_struct(name, value))
    }

    fn serialize_newtype_variant<V: Serialize + ?Sized>(&mut self, name: &'static str, variant_index: u32, variant: &'static str, value: &V) -> Result<()> {
        dispatch!(self, Self(builder) => builder.serialize_newtype_variant(name, variant_index, variant, value))
    }

    fn serialize_unit_variant(&mut self, name: &'static str, variant_index: u32, variant: &'static str) -> Result<()> {
        dispatch!(self, Self(builder) => builder.serialize_unit_variant(name, variant_index, variant))
    }

    fn serialize_struct_variant_start<'this>(&'this mut self, name: &'static str, variant_index: u32, variant: &'static str, len: usize) -> Result<&'this mut ArrayBuilder> {
        dispatch!(self, Self(builder) => builder.serialize_struct_variant_start(name, variant_index, variant, len))
    }

    fn serialize_tuple_variant_start<'this> (&'this mut self, name: &'static str, variant_index: u32, variant: &'static str, len: usize) -> Result<&'this mut ArrayBuilder> {
        dispatch!(self, Self(builder) => builder.serialize_tuple_variant_start(name, variant_index, variant, len))
    }
}
