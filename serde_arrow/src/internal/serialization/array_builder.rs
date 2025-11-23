use std::collections::BTreeMap;

use half::f16;
use serde::Serialize;

use marrow::{
    array::{Array, BytesArray, BytesViewArray},
    datatypes::FieldMeta,
};

use crate::internal::error::{Context, Error, Result};

use super::{
    binary_builder::BinaryBuilder, bool_builder::BoolBuilder, date_builder::DateBuilder,
    decimal_builder::DecimalBuilder, dictionary_utf8_builder::DictionaryUtf8Builder,
    duration_builder::DurationBuilder, fixed_size_binary_builder::FixedSizeBinaryBuilder,
    fixed_size_list_builder::FixedSizeListBuilder, float_builder::FloatBuilder,
    int_builder::IntBuilder, list_builder::ListBuilder, map_builder::MapBuilder,
    null_builder::NullBuilder, struct_builder::StructBuilder, time_builder::TimeBuilder,
    timestamp_builder::TimestampBuilder, union_builder::UnionBuilder,
    unknown_variant_builder::UnknownVariantBuilder, utf8_builder::Utf8Builder,
};

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
    Date32(DateBuilder<i32>),
    Date64(DateBuilder<i64>),
    Time32(TimeBuilder<i32>),
    Time64(TimeBuilder<i64>),
    Duration(DurationBuilder),
    Timestamp(TimestampBuilder),
    Decimal128(DecimalBuilder),
    List(ListBuilder<i32>),
    LargeList(ListBuilder<i64>),
    FixedSizedList(FixedSizeListBuilder),
    Binary(BinaryBuilder<BytesArray<i32>>),
    LargeBinary(BinaryBuilder<BytesArray<i64>>),
    BinaryView(BinaryBuilder<BytesViewArray>),
    FixedSizeBinary(FixedSizeBinaryBuilder),
    Map(MapBuilder),
    Struct(StructBuilder),
    Utf8(Utf8Builder<BytesArray<i32>>),
    LargeUtf8(Utf8Builder<BytesArray<i64>>),
    Utf8View(Utf8Builder<BytesViewArray>),
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
            $wrapper::Time32($name) => $expr,
            $wrapper::Time64($name) => $expr,
            $wrapper::Duration($name) => $expr,
            $wrapper::Timestamp($name) => $expr,
            $wrapper::Decimal128($name) => $expr,
            $wrapper::Utf8($name) => $expr,
            $wrapper::LargeUtf8($name) => $expr,
            $wrapper::Utf8View($name) => $expr,
            $wrapper::List($name) => $expr,
            $wrapper::LargeList($name) => $expr,
            $wrapper::FixedSizedList($name) => $expr,
            $wrapper::Binary($name) => $expr,
            $wrapper::LargeBinary($name) => $expr,
            $wrapper::BinaryView($name) => $expr,
            $wrapper::FixedSizeBinary($name) => $expr,
            $wrapper::Map($name) => $expr,
            $wrapper::Struct($name) => $expr,
            $wrapper::DictionaryUtf8($name) => $expr,
            $wrapper::Union($name) => $expr,
            $wrapper::UnknownVariant($name) => $expr,
        }
    };
}

impl ArrayBuilder {
    pub fn get_name(&self) -> &str {
        dispatch!(self, Self(builder) => &builder.name)
    }

    pub fn is_nullable(&self) -> bool {
        dispatch!(self, Self(builder) => builder.is_nullable())
    }

    pub fn into_array_and_field_meta(self) -> Result<(Array, FieldMeta)> {
        dispatch!(self, Self(builder) => builder.into_array_and_field_meta())
    }

    pub fn take(&mut self) -> ArrayBuilder {
        dispatch!(self, Self(builder) => builder.take())
    }

    pub fn reserve(&mut self, additional: usize) {
        dispatch!(self, Self(builder) => builder.reserve(additional))
    }

    pub fn serialize_default_value(&mut self) -> Result<()> {
        dispatch!(self, Self(builder) => builder.serialize_default_value())
    }
}

impl Context for ArrayBuilder {
    fn annotate(&self, annotations: &mut BTreeMap<String, String>) {
        dispatch!(self, Self(builder) => builder.annotate(annotations))
    }
}

impl<'a> serde::Serializer for &'a mut ArrayBuilder {
    type Ok = ();
    type Error = Error;

    type SerializeStruct = super::utils::SerializeStruct<'a>;
    type SerializeStructVariant = super::utils::SerializeStruct<'a>;
    type SerializeTuple = super::utils::SerializeTuple<'a>;
    type SerializeTupleVariant = super::utils::SerializeTuple<'a>;
    type SerializeTupleStruct = super::utils::SerializeTuple<'a>;
    type SerializeSeq = super::utils::SerializeSeq<'a>;
    type SerializeMap = super::utils::SerializeMap<'a>;

    fn serialize_none(self) -> Result<()> {
        dispatch!(self, ArrayBuilder(builder) => builder.serialize_none())
    }

    fn serialize_some<T: ?Sized + Serialize>(self, value: &T) -> Result<()> {
        dispatch!(self, ArrayBuilder(builder) => builder.serialize_some(value))
    }

    fn serialize_unit(self) -> Result<()> {
        dispatch!(self, ArrayBuilder(builder) => builder.serialize_unit())
    }

    fn serialize_bool(self, v: bool) -> Result<()> {
        dispatch!(self, ArrayBuilder(builder) => builder.serialize_bool(v))
    }

    fn serialize_char(self, v: char) -> Result<()> {
        dispatch!(self, ArrayBuilder(builder) => builder.serialize_char(v))
    }

    fn serialize_i8(self, v: i8) -> Result<()> {
        dispatch!(self, ArrayBuilder(builder) => builder.serialize_i8(v))
    }

    fn serialize_i16(self, v: i16) -> Result<()> {
        dispatch!(self, ArrayBuilder(builder) => builder.serialize_i16(v))
    }

    fn serialize_i32(self, v: i32) -> Result<()> {
        dispatch!(self, ArrayBuilder(builder) => builder.serialize_i32(v))
    }

    fn serialize_i64(self, v: i64) -> Result<()> {
        dispatch!(self, ArrayBuilder(builder) => builder.serialize_i64(v))
    }

    fn serialize_u8(self, v: u8) -> Result<()> {
        dispatch!(self, ArrayBuilder(builder) => builder.serialize_u8(v))
    }

    fn serialize_u16(self, v: u16) -> Result<()> {
        dispatch!(self, ArrayBuilder(builder) => builder.serialize_u16(v))
    }

    fn serialize_u32(self, v: u32) -> Result<()> {
        dispatch!(self, ArrayBuilder(builder) => builder.serialize_u32(v))
    }

    fn serialize_u64(self, v: u64) -> Result<()> {
        dispatch!(self, ArrayBuilder(builder) => builder.serialize_u64(v))
    }

    fn serialize_f32(self, v: f32) -> Result<()> {
        dispatch!(self, ArrayBuilder(builder) => builder.serialize_f32(v))
    }

    fn serialize_f64(self, v: f64) -> Result<()> {
        dispatch!(self, ArrayBuilder(builder) => builder.serialize_f64(v))
    }

    fn serialize_str(self, v: &str) -> Result<()> {
        dispatch!(self, ArrayBuilder(builder) => builder.serialize_str(v))
    }

    fn serialize_bytes(self, v: &[u8]) -> Result<()> {
        dispatch!(self, ArrayBuilder(builder) => builder.serialize_bytes(v))
    }

    fn serialize_seq(self, len: Option<usize>) -> Result<Self::SerializeSeq> {
        dispatch!(self, ArrayBuilder(builder) => builder.serialize_seq(len))
    }

    fn serialize_map(self, len: Option<usize>) -> Result<Self::SerializeMap> {
        dispatch!(self, ArrayBuilder(builder) => builder.serialize_map(len))
    }

    fn serialize_tuple(self, len: usize) -> Result<Self::SerializeTuple> {
        dispatch!(self, ArrayBuilder(builder) => builder.serialize_tuple(len))
    }

    fn serialize_struct(self, name: &'static str, len: usize) -> Result<Self::SerializeStruct> {
        dispatch!(self, ArrayBuilder(builder) => builder.serialize_struct(name, len))
    }

    fn serialize_unit_struct(self, name: &'static str) -> Result<()> {
        dispatch!(self, ArrayBuilder(builder) => builder.serialize_unit_struct(name))
    }

    fn serialize_unit_variant(
        self,
        name: &'static str,
        variant_index: u32,
        variant: &'static str,
    ) -> Result<()> {
        dispatch!(self, ArrayBuilder(builder) => builder.serialize_unit_variant(name, variant_index, variant))
    }

    fn serialize_newtype_struct<T: ?Sized + Serialize>(
        self,
        name: &'static str,
        value: &T,
    ) -> Result<()> {
        dispatch!(self, ArrayBuilder(builder) => builder.serialize_newtype_struct(name, value))
    }

    fn serialize_newtype_variant<T: ?Sized + Serialize>(
        self,
        name: &'static str,
        variant_index: u32,
        variant: &'static str,
        value: &T,
    ) -> Result<()> {
        dispatch!(self, ArrayBuilder(builder) => builder.serialize_newtype_variant(name, variant_index, variant, value))
    }

    fn serialize_tuple_struct(
        self,
        name: &'static str,
        len: usize,
    ) -> Result<Self::SerializeTupleStruct> {
        dispatch!(self, ArrayBuilder(builder) => builder.serialize_tuple_struct(name, len))
    }

    fn serialize_tuple_variant(
        self,
        name: &'static str,
        variant_index: u32,
        variant: &'static str,
        len: usize,
    ) -> Result<Self::SerializeTupleVariant> {
        dispatch!(self, ArrayBuilder(builder) => builder.serialize_tuple_variant(name, variant_index, variant, len))
    }

    fn serialize_struct_variant(
        self,
        name: &'static str,
        variant_index: u32,
        variant: &'static str,
        len: usize,
    ) -> Result<Self::SerializeStructVariant> {
        dispatch!(self, ArrayBuilder(builder) => builder.serialize_struct_variant(name, variant_index, variant, len))
    }
}
