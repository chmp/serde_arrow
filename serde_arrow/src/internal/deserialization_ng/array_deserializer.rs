use crate::Result;

use super::list_deserializer::ListDeserializer;
use super::primitive_deserializer::PrimitiveDeserializer;
use super::simple_deserializer::SimpleDeserializer;
use super::struct_deserializer::StructDeserializer;


pub enum ArrayDeserializer<'a> {
    I8(PrimitiveDeserializer<'a, i8>),
    I16(PrimitiveDeserializer<'a, i16>),
    I32(PrimitiveDeserializer<'a, i32>),
    I64(PrimitiveDeserializer<'a, i64>),
    Struct(StructDeserializer<'a>),
    List(ListDeserializer<'a, i32>),
    LargeList(ListDeserializer<'a, i64>),
}

impl<'a> From<PrimitiveDeserializer<'a, i8>> for ArrayDeserializer<'a> {
    fn from(value: PrimitiveDeserializer<'a, i8>) -> Self {
        Self::I8(value)
    } 
}

impl<'a> From<PrimitiveDeserializer<'a, i16>> for ArrayDeserializer<'a> {
    fn from(value: PrimitiveDeserializer<'a, i16>) -> Self {
        Self::I16(value)
    } 
}

impl<'a> From<PrimitiveDeserializer<'a, i32>> for ArrayDeserializer<'a> {
    fn from(value: PrimitiveDeserializer<'a, i32>) -> Self {
        Self::I32(value)
    } 
}

impl<'a> From<PrimitiveDeserializer<'a, i64>> for ArrayDeserializer<'a> {
    fn from(value: PrimitiveDeserializer<'a, i64>) -> Self {
        Self::I64(value)
    } 
}

impl<'a> From<StructDeserializer<'a>> for ArrayDeserializer<'a> {
    fn from(value: StructDeserializer<'a>) -> Self {
        Self::Struct(value)
    }
}

impl<'a> From<ListDeserializer<'a, i32>> for ArrayDeserializer<'a> {
    fn from(value: ListDeserializer<'a, i32>) -> Self {
        Self::List(value)
    }
}

impl<'a> From<ListDeserializer<'a, i64>> for ArrayDeserializer<'a> {
    fn from(value: ListDeserializer<'a, i64>) -> Self {
        Self::LargeList(value)
    }
}

macro_rules! dispatch {
    ($obj:expr, $wrapper:ident($name:ident) => $expr:expr) => {
        match $obj {
            $wrapper::I8($name) => $expr,
            $wrapper::I16($name) => $expr,
            $wrapper::I32($name) => $expr,
            $wrapper::I64($name) => $expr,
            $wrapper::Struct($name) => $expr,
            $wrapper::List($name) => $expr,
            $wrapper::LargeList($name) => $expr,
        }
    };
}

impl<'de> SimpleDeserializer<'de> for ArrayDeserializer<'de> {
    fn name() -> &'static str {
        "ArrayDeserializer"
    }

    fn deserialize_any<V: serde::de::Visitor<'de>>(&mut self, visitor: V) -> Result<V::Value> {
        dispatch!(self, ArrayDeserializer(deser) => deser.deserialize_any(visitor))
    }

    fn deserialize_i8<V: serde::de::Visitor<'de>>(&mut self, visitor: V) -> Result<V::Value> {
        dispatch!(self, ArrayDeserializer(deser) => deser.deserialize_i8(visitor))
    }

    fn deserialize_i16<V: serde::de::Visitor<'de>>(&mut self, visitor: V) -> Result<V::Value> {
        dispatch!(self, ArrayDeserializer(deser) => deser.deserialize_i16(visitor))
    }

    fn deserialize_i32<V: serde::de::Visitor<'de>>(&mut self, visitor: V) -> Result<V::Value> {
        dispatch!(self, ArrayDeserializer(deser) => deser.deserialize_i32(visitor))
    }

    fn deserialize_i64<V: serde::de::Visitor<'de>>(&mut self, visitor: V) -> Result<V::Value> {
        dispatch!(self, ArrayDeserializer(deser) => deser.deserialize_i64(visitor))
    }

    fn deserialize_struct<V: serde::de::Visitor<'de>>(
        &mut self,
        name: &'static str,
        fields: &'static [&'static str],
        visitor: V,
    ) -> Result<V::Value> {
        dispatch!(self, ArrayDeserializer(deser) => deser.deserialize_struct(name, fields, visitor))
    }

    fn deserialize_seq<V: serde::de::Visitor<'de>>(&mut self, visitor: V) -> Result<V::Value> {
        dispatch!(self, ArrayDeserializer(deser) => deser.deserialize_seq(visitor))
    }
}
