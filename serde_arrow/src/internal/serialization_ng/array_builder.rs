use serde::Serialize;

use crate::{internal::common::MutableBitBuffer, Result};

use super::{
    i8_builder::I8Builder, list_builder::ListBuilder, struct_builder::StructBuilder,
    utils::SimpleSerializer,
};

#[derive(Debug, Clone)]
pub enum ArrayBuilder {
    List(ListBuilder),
    I8(I8Builder),
    Struct(StructBuilder),
}

impl ArrayBuilder {
    pub fn i8(is_nullable: bool) -> Self {
        Self::I8(I8Builder {
            validity: is_nullable.then(MutableBitBuffer::default),
            buffer: Default::default(),
        })
    }

    pub fn list(element: ArrayBuilder, is_nullable: bool) -> Self {
        Self::List(ListBuilder::new(element, is_nullable))
    }

    pub fn r#struct(named_fields: Vec<(String, ArrayBuilder)>, is_nullable: bool) -> Result<Self> {
        Ok(Self::Struct(StructBuilder::new(named_fields, is_nullable)?))
    }
}

impl ArrayBuilder {
    pub fn unwrap_list(self) -> ListBuilder {
        match self {
            Self::List(builder) => builder,
            _ => panic!(),
        }
    }

    pub fn unwrap_i8(self) -> I8Builder {
        match self {
            Self::I8(builder) => builder,
            _ => panic!(),
        }
    }

    pub fn unwrap_struct(self) -> StructBuilder {
        match self {
            Self::Struct(builder) => builder,
            _ => panic!(),
        }
    }
}

macro_rules! dispatch {
    ($obj:expr, $wrapper:ident($name:ident) => $expr:expr) => {
        match $obj {
            $wrapper::List($name) => $expr,
            $wrapper::I8($name) => $expr,
            $wrapper::Struct($name) => $expr,
        }
    };
}

impl ArrayBuilder {
    pub fn serialize_default(&mut self) -> Result<()> {
        dispatch!(self, ArrayBuilder(builder) => builder.serialize_default())
    }
}

impl SimpleSerializer for ArrayBuilder {
    fn name(&self) -> &str {
        "ArrayBuilder"
    }

    fn serialize_i8(&mut self, v: i8) -> Result<()> {
        dispatch!(self, Self(builder) => builder.serialize_i8(v))
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
}
