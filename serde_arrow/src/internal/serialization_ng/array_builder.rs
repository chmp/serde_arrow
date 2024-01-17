use serde::{
    ser::{SerializeSeq, SerializeStruct},
    Serialize, Serializer,
};

use crate::{Error, Result};

use super::{
    i8_builder::I8Builder, list_builder::ListBuilder, not_implemented::NotImplemented,
    struct_builder::StructBuilder,
};

#[derive(Debug, Clone)]
pub enum ArrayBuilder {
    List(ListBuilder),
    I8(I8Builder),
    Struct(StructBuilder),
}

impl ArrayBuilder {
    pub fn i8() -> Self {
        Self::I8(I8Builder::default())
    }

    pub fn list(element: ArrayBuilder) -> Self {
        Self::List(ListBuilder::new(element))
    }

    pub fn r#struct(named_fields: Vec<(String, ArrayBuilder)>) -> Result<Self> {
        Ok(Self::Struct(StructBuilder::new(named_fields)?))
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

macro_rules! dispatch_builder {
    ($obj:expr, $wrapper:ident($name:ident) => $expr:expr) => {
        match $obj {
            $wrapper::List($name) => $expr,
            $wrapper::I8($name) => $expr,
            $wrapper::Struct($name) => $expr,
        }
    };
}

macro_rules! dispatch_builder_wrapped {
    ($obj:expr, $arg_wrapper:ident($name:ident) => $result_wrapper:ident($expr:expr)) => {
        match $obj {
            $arg_wrapper::List($name) => $result_wrapper::List($expr),
            $arg_wrapper::I8($name) => $result_wrapper::I8($expr),
            $arg_wrapper::Struct($name) => $result_wrapper::Struct($expr),
        }
    };
}

impl<'a> serde::Serializer for &'a mut ArrayBuilder {
    type Error = Error;
    type Ok = ();

    type SerializeSeq = ArrayBuilderSerializeSeq<'a>;
    type SerializeStruct = ArrayBuilderSerializeStruct<'a>;
    type SerializeMap = NotImplemented;
    type SerializeTuple = NotImplemented;
    type SerializeStructVariant = NotImplemented;
    type SerializeTupleStruct = NotImplemented;
    type SerializeTupleVariant = NotImplemented;

    fn serialize_unit(self) -> Result<()> {
        dispatch_builder!(self, ArrayBuilder(builder) => builder.serialize_unit())
    }

    fn serialize_none(self) -> Result<()> {
        dispatch_builder!(self, ArrayBuilder(builder) => builder.serialize_none())
    }

    fn serialize_some<T: Serialize + ?Sized>(self, value: &T) -> Result<()> {
        dispatch_builder!(self, ArrayBuilder(builder) => builder.serialize_some(value))
    }

    fn serialize_bool(self, v: bool) -> Result<()> {
        dispatch_builder!(self, ArrayBuilder(builder) => builder.serialize_bool(v))
    }

    fn serialize_char(self, v: char) -> Result<()> {
        dispatch_builder!(self, ArrayBuilder(builder) => builder.serialize_char(v))
    }

    fn serialize_u8(self, v: u8) -> Result<()> {
        dispatch_builder!(self, ArrayBuilder(builder) => builder.serialize_u8(v))
    }

    fn serialize_u16(self, v: u16) -> Result<()> {
        dispatch_builder!(self, ArrayBuilder(builder) => builder.serialize_u16(v))
    }

    fn serialize_u32(self, v: u32) -> Result<()> {
        dispatch_builder!(self, ArrayBuilder(builder) => builder.serialize_u32(v))
    }

    fn serialize_u64(self, v: u64) -> Result<()> {
        dispatch_builder!(self, ArrayBuilder(builder) => builder.serialize_u64(v))
    }

    fn serialize_i8(self, v: i8) -> Result<()> {
        dispatch_builder!(self, ArrayBuilder(builder) => builder.serialize_i8(v))
    }

    fn serialize_i16(self, v: i16) -> Result<()> {
        dispatch_builder!(self, ArrayBuilder(builder) => builder.serialize_i16(v))
    }

    fn serialize_i32(self, v: i32) -> Result<()> {
        dispatch_builder!(self, ArrayBuilder(builder) => builder.serialize_i32(v))
    }

    fn serialize_i64(self, v: i64) -> Result<()> {
        dispatch_builder!(self, ArrayBuilder(builder) => builder.serialize_i64(v))
    }

    fn serialize_f32(self, v: f32) -> Result<()> {
        dispatch_builder!(self, ArrayBuilder(builder) => builder.serialize_f32(v))
    }

    fn serialize_f64(self, v: f64) -> Result<()> {
        dispatch_builder!(self, ArrayBuilder(builder) => builder.serialize_f64(v))
    }

    fn serialize_bytes(self, v: &[u8]) -> Result<()> {
        dispatch_builder!(self, ArrayBuilder(builder) => builder.serialize_bytes(v))
    }

    fn serialize_str(self, v: &str) -> Result<()> {
        dispatch_builder!(self, ArrayBuilder(builder) => builder.serialize_str(v))
    }

    fn serialize_newtype_struct<T: Serialize + ?Sized>(
        self,
        name: &'static str,
        value: &T,
    ) -> Result<()> {
        dispatch_builder!(self, ArrayBuilder(builder) => builder.serialize_newtype_struct(name, value))
    }

    fn serialize_newtype_variant<T: Serialize + ?Sized>(
        self,
        name: &'static str,
        variant_index: u32,
        variant: &'static str,
        value: &T,
    ) -> Result<()> {
        dispatch_builder!(self, ArrayBuilder(builder) => builder.serialize_newtype_variant(name, variant_index, variant, value))
    }

    fn serialize_unit_struct(self, name: &'static str) -> Result<()> {
        dispatch_builder!(self, ArrayBuilder(builder) => builder.serialize_unit_struct(name))
    }

    fn serialize_unit_variant(
        self,
        name: &'static str,
        variant_index: u32,
        variant: &'static str,
    ) -> Result<()> {
        dispatch_builder!(self, ArrayBuilder(builder) => builder.serialize_unit_variant(name, variant_index, variant))
    }

    fn serialize_map(self, _len: Option<usize>) -> Result<Self::SerializeMap> {
        Ok(NotImplemented)
    }

    fn serialize_seq(self, len: Option<usize>) -> Result<Self::SerializeSeq> {
        Ok(
            dispatch_builder_wrapped!(self, ArrayBuilder(builder) => ArrayBuilderSerializeSeq(builder.serialize_seq(len)?)),
        )
    }

    fn serialize_struct(self, name: &'static str, len: usize) -> Result<Self::SerializeStruct> {
        Ok(
            dispatch_builder_wrapped!(self, ArrayBuilder(builder) => ArrayBuilderSerializeStruct(builder.serialize_struct(name, len)?)),
        )
    }

    fn serialize_tuple(self, _len: usize) -> Result<Self::SerializeTuple> {
        Ok(NotImplemented)
    }

    fn serialize_struct_variant(
        self,
        _name: &'static str,
        _variant_index: u32,
        _variant: &'static str,
        _len: usize,
    ) -> Result<Self::SerializeStructVariant> {
        Ok(NotImplemented)
    }

    fn serialize_tuple_struct(
        self,
        _name: &'static str,
        _len: usize,
    ) -> Result<Self::SerializeTupleStruct> {
        Ok(NotImplemented)
    }

    fn serialize_tuple_variant(
        self,
        _name: &'static str,
        _variant_index: u32,
        _variant: &'static str,
        _len: usize,
    ) -> Result<Self::SerializeTupleVariant> {
        Ok(NotImplemented)
    }
}

pub enum ArrayBuilderSerializeSeq<'a> {
    List(<&'a mut ListBuilder as Serializer>::SerializeSeq),
    I8(<&'a mut I8Builder as Serializer>::SerializeSeq),
    Struct(<&'a mut StructBuilder as Serializer>::SerializeSeq),
}

impl<'a> SerializeSeq for ArrayBuilderSerializeSeq<'a> {
    type Ok = ();
    type Error = Error;

    fn serialize_element<T: Serialize + ?Sized>(&mut self, value: &T) -> Result<()> {
        dispatch_builder!(self, Self(builder) => builder.serialize_element(value))
    }

    fn end(self) -> std::prelude::v1::Result<Self::Ok, Self::Error> {
        dispatch_builder!(self, Self(builder) => SerializeSeq::end(builder))
    }
}

pub enum ArrayBuilderSerializeStruct<'a> {
    List(<&'a mut ListBuilder as Serializer>::SerializeStruct),
    I8(<&'a mut I8Builder as Serializer>::SerializeStruct),
    Struct(<&'a mut StructBuilder as Serializer>::SerializeStruct),
}

impl<'a> SerializeStruct for ArrayBuilderSerializeStruct<'a> {
    type Ok = ();
    type Error = Error;

    fn serialize_field<T: Serialize + ?Sized>(
        &mut self,
        key: &'static str,
        value: &T,
    ) -> Result<()> {
        dispatch_builder!(self, Self(builder) => builder.serialize_field(key, value))
    }

    fn end(self) -> Result<()> {
        dispatch_builder!(self, Self(builder) => SerializeStruct::end(builder))
    }
}
