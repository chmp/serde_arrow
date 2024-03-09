use crate::Result;

use super::bool_deserializer::BoolDeserializer;
use super::list_deserializer::ListDeserializer;
use super::null_deserializer::NullDeserializer;
use super::primitive_deserializer::PrimitiveDeserializer;
use super::simple_deserializer::SimpleDeserializer;
use super::string_deserializer::StringDeserializer;
use super::struct_deserializer::StructDeserializer;

pub enum ArrayDeserializer<'a> {
    Null(NullDeserializer),
    Bool(BoolDeserializer<'a>),
    U8(PrimitiveDeserializer<'a, u8>),
    U16(PrimitiveDeserializer<'a, u16>),
    U32(PrimitiveDeserializer<'a, u32>),
    U64(PrimitiveDeserializer<'a, u64>),
    I8(PrimitiveDeserializer<'a, i8>),
    I16(PrimitiveDeserializer<'a, i16>),
    I32(PrimitiveDeserializer<'a, i32>),
    I64(PrimitiveDeserializer<'a, i64>),
    Utf8(StringDeserializer<'a, i32>),
    LargeUtf8(StringDeserializer<'a, i64>),
    Struct(StructDeserializer<'a>),
    List(ListDeserializer<'a, i32>),
    LargeList(ListDeserializer<'a, i64>),
}

impl<'a> From<NullDeserializer> for ArrayDeserializer<'a> {
    fn from(value: NullDeserializer) -> Self {
        Self::Null(value)
    }
}

impl<'a> From<BoolDeserializer<'a>> for ArrayDeserializer<'a> {
    fn from(value: BoolDeserializer<'a>) -> Self {
        Self::Bool(value)
    }
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

impl<'a> From<PrimitiveDeserializer<'a, u8>> for ArrayDeserializer<'a> {
    fn from(value: PrimitiveDeserializer<'a, u8>) -> Self {
        Self::U8(value)
    }
}

impl<'a> From<PrimitiveDeserializer<'a, u16>> for ArrayDeserializer<'a> {
    fn from(value: PrimitiveDeserializer<'a, u16>) -> Self {
        Self::U16(value)
    }
}

impl<'a> From<PrimitiveDeserializer<'a, u32>> for ArrayDeserializer<'a> {
    fn from(value: PrimitiveDeserializer<'a, u32>) -> Self {
        Self::U32(value)
    }
}

impl<'a> From<PrimitiveDeserializer<'a, u64>> for ArrayDeserializer<'a> {
    fn from(value: PrimitiveDeserializer<'a, u64>) -> Self {
        Self::U64(value)
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

impl<'a> From<StringDeserializer<'a, i32>> for ArrayDeserializer<'a> {
    fn from(value: StringDeserializer<'a, i32>) -> Self {
        Self::Utf8(value)
    }
}

impl<'a> From<StringDeserializer<'a, i64>> for ArrayDeserializer<'a> {
    fn from(value: StringDeserializer<'a, i64>) -> Self {
        Self::LargeUtf8(value)
    }
}

macro_rules! dispatch {
    ($obj:expr, $wrapper:ident($name:ident) => $expr:expr) => {
        match $obj {
            $wrapper::Null($name) => $expr,
            $wrapper::Bool($name) => $expr,
            $wrapper::U8($name) => $expr,
            $wrapper::U16($name) => $expr,
            $wrapper::U32($name) => $expr,
            $wrapper::U64($name) => $expr,
            $wrapper::I8($name) => $expr,
            $wrapper::I16($name) => $expr,
            $wrapper::I32($name) => $expr,
            $wrapper::I64($name) => $expr,
            $wrapper::Utf8($name) => $expr,
            $wrapper::LargeUtf8($name) => $expr,
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

    fn deserialize_unit<V: serde::de::Visitor<'de>>(&mut self, visitor: V) -> Result<V::Value> {
        dispatch!(self, ArrayDeserializer(deser) => deser.deserialize_unit(visitor))
    }

    fn deserialize_unit_struct<V: serde::de::Visitor<'de>>(
        &mut self,
        name: &'static str,
        visitor: V,
    ) -> Result<V::Value> {
        dispatch!(self, ArrayDeserializer(deser) => deser.deserialize_unit_struct(name, visitor))
    }

    fn deserialize_bool<V: serde::de::Visitor<'de>>(&mut self, visitor: V) -> Result<V::Value> {
        dispatch!(self, ArrayDeserializer(deser) => deser.deserialize_bool(visitor))
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

    fn deserialize_str<V: serde::de::Visitor<'de>>(&mut self, visitor: V) -> Result<V::Value> {
        dispatch!(self, ArrayDeserializer(deser) => deser.deserialize_str(visitor))
    }

    fn deserialize_string<V: serde::de::Visitor<'de>>(&mut self, visitor: V) -> Result<V::Value> {
        dispatch!(self, ArrayDeserializer(deser) => deser.deserialize_string(visitor))
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
