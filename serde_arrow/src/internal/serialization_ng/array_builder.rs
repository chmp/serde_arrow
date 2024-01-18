use serde::Serialize;

use crate::{internal::common::MutableBitBuffer, Result};

use super::{
    i8_builder::I8Builder, list_builder::ListBuilder, map_builder::MapBuilder,
    struct_builder::StructBuilder, utf8_builder::Utf8Builder, utils::SimpleSerializer,
};

// TODO: add outer sequence builder? (not limited by i64 limits)
#[derive(Debug, Clone)]
pub enum ArrayBuilder {
    I8(I8Builder),
    List(ListBuilder<i32>),
    LargeList(ListBuilder<i64>),
    Map(MapBuilder),
    Struct(StructBuilder),
    Utf8(Utf8Builder<i32>),
    LargeUtf8(Utf8Builder<i64>),
}

macro_rules! dispatch {
    ($obj:expr, $wrapper:ident($name:ident) => $expr:expr) => {
        match $obj {
            $wrapper::I8($name) => $expr,
            $wrapper::Utf8($name) => $expr,
            $wrapper::LargeUtf8($name) => $expr,
            $wrapper::List($name) => $expr,
            $wrapper::LargeList($name) => $expr,
            $wrapper::Map($name) => $expr,
            $wrapper::Struct($name) => $expr,
        }
    };
}

impl ArrayBuilder {
    pub fn i8(is_nullable: bool) -> Self {
        Self::I8(I8Builder::new(is_nullable))
    }

    pub fn utf8(is_nullable: bool) -> Self {
        Self::Utf8(Utf8Builder::new(is_nullable))
    }

    pub fn list(element: ArrayBuilder, is_nullable: bool) -> Self {
        Self::List(ListBuilder::new(element, is_nullable))
    }

    pub fn large_list(element: ArrayBuilder, is_nullable: bool) -> Self {
        Self::LargeList(ListBuilder::new(element, is_nullable))
    }

    pub fn map(key: ArrayBuilder, value: ArrayBuilder, is_nullable: bool) -> Self {
        Self::Map(MapBuilder::new(key, value, is_nullable))
    }

    pub fn r#struct(named_fields: Vec<(String, ArrayBuilder)>, is_nullable: bool) -> Result<Self> {
        Ok(Self::Struct(StructBuilder::new(named_fields, is_nullable)?))
    }
}

impl ArrayBuilder {
    pub fn unwrap_i8(self) -> (Option<MutableBitBuffer>, Vec<i8>) {
        match self {
            Self::I8(builder) => (builder.validity, builder.buffer),
            _ => panic!(),
        }
    }

    pub fn unwrap_utf8(self) -> (Option<MutableBitBuffer>, Vec<i32>, Vec<u8>) {
        match self {
            Self::Utf8(builder) => (builder.validity, builder.offsets.offsets, builder.buffer),
            _ => panic!(),
        }
    }

    pub fn unwrap_large_utf8(self) -> (Option<MutableBitBuffer>, Vec<i64>, Vec<u8>) {
        match self {
            Self::LargeUtf8(builder) => (builder.validity, builder.offsets.offsets, builder.buffer),
            _ => panic!(),
        }
    }

    pub fn unwrap_list(self) -> (Option<MutableBitBuffer>, Vec<i32>, ArrayBuilder) {
        match self {
            Self::List(builder) => (builder.validity, builder.offsets.offsets, *builder.element),
            _ => panic!(),
        }
    }

    pub fn unwrap_large_list(self) -> (Option<MutableBitBuffer>, Vec<i64>, ArrayBuilder) {
        match self {
            Self::LargeList(builder) => {
                (builder.validity, builder.offsets.offsets, *builder.element)
            }
            _ => panic!(),
        }
    }

    pub fn unwrap_map(
        self,
    ) -> (
        Option<MutableBitBuffer>,
        Vec<i32>,
        ArrayBuilder,
        ArrayBuilder,
    ) {
        match self {
            Self::Map(builder) => (
                builder.validity,
                builder.offsets.offsets,
                *builder.key,
                *builder.value,
            ),
            _ => panic!(),
        }
    }

    pub fn unwrap_struct(self) -> (Option<MutableBitBuffer>, Vec<String>, Vec<ArrayBuilder>) {
        match self {
            Self::Struct(builder) => {
                let mut names = Vec::new();
                let mut fields = Vec::new();

                for (name, field) in builder.named_fields {
                    names.push(name);
                    fields.push(field);
                }

                (builder.validity, names, fields)
            }
            _ => panic!(),
        }
    }
}

impl SimpleSerializer for ArrayBuilder {
    fn name(&self) -> &str {
        "ArrayBuilder"
    }

    fn serialize_i8(&mut self, v: i8) -> Result<()> {
        dispatch!(self, Self(builder) => builder.serialize_i8(v))
    }

    fn serialize_str(&mut self, v: &str) -> Result<()> {
        dispatch!(self, Self(builder) => builder.serialize_str(v))
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

    fn serialize_struct_field<V: Serialize + ?Sized>(
        &mut self,
        key: &'static str,
        value: &V,
    ) -> Result<()> {
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
}
