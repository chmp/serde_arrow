use crate::Result;

use super::list_deserializer::ListDeserializer;
use super::primitive_deserializer::{Primitive, PrimitiveDeserializer};
use super::simple_deserializer::SimpleDeserializer;
use super::struct_deserializer::StructDeserializer;

pub enum ArrayDeserializer<'a> {
    U8(PrimitiveDeserializer<'a, u8>),
    U16(PrimitiveDeserializer<'a, u16>),
    U32(PrimitiveDeserializer<'a, u32>),
    U64(PrimitiveDeserializer<'a, u64>),
    I8(PrimitiveDeserializer<'a, i8>),
    I16(PrimitiveDeserializer<'a, i16>),
    I32(PrimitiveDeserializer<'a, i32>),
    I64(PrimitiveDeserializer<'a, i64>),
    Struct(StructDeserializer<'a>),
    List(ListDeserializer<'a, i32>),
    LargeList(ListDeserializer<'a, i64>),
}

impl<'a, T: Primitive> From<PrimitiveDeserializer<'a, T>> for ArrayDeserializer<'a> {
    fn from(value: PrimitiveDeserializer<'a, T>) -> Self {
        T::build_array_deserializer(value)
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
            $wrapper::U8($name) => $expr,
            $wrapper::U16($name) => $expr,
            $wrapper::U32($name) => $expr,
            $wrapper::U64($name) => $expr,
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

    fn deserialize_option<V: serde::de::Visitor<'de>>(&mut self, visitor: V) -> Result<V::Value> {
        dispatch!(self, ArrayDeserializer(deser) => deser.deserialize_option(visitor))
    }

    fn deserialize_char<V: serde::de::Visitor<'de>>(&mut self, visitor: V) -> Result<V::Value> {
        dispatch!(self, ArrayDeserializer(deser) => deser.deserialize_char(visitor))
    }

    fn deserialize_u8<V: serde::de::Visitor<'de>>(&mut self, visitor: V) -> Result<V::Value> {
        dispatch!(self, ArrayDeserializer(deser) => deser.deserialize_u8(visitor))
    }

    fn deserialize_u16<V: serde::de::Visitor<'de>>(&mut self, visitor: V) -> Result<V::Value> {
        dispatch!(self, ArrayDeserializer(deser) => deser.deserialize_u16(visitor))
    }

    fn deserialize_u32<V: serde::de::Visitor<'de>>(&mut self, visitor: V) -> Result<V::Value> {
        dispatch!(self, ArrayDeserializer(deser) => deser.deserialize_u32(visitor))
    }

    fn deserialize_u64<V: serde::de::Visitor<'de>>(&mut self, visitor: V) -> Result<V::Value> {
        dispatch!(self, ArrayDeserializer(deser) => deser.deserialize_u64(visitor))
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

    fn deserialize_f32<V: serde::de::Visitor<'de>>(&mut self, visitor: V) -> Result<V::Value> {
        dispatch!(self, ArrayDeserializer(deser) => deser.deserialize_f32(visitor))
    }

    fn deserialize_f64<V: serde::de::Visitor<'de>>(&mut self, visitor: V) -> Result<V::Value> {
        dispatch!(self, ArrayDeserializer(deser) => deser.deserialize_f64(visitor))
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
