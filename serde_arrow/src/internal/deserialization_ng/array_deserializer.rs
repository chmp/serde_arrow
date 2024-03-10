use serde::de::Visitor;

use crate::internal::error::Result;

use super::{
    bool_deserializer::BoolDeserializer, date64_deserializer::Date64Deserializer,
    float_deserializer::FloatDeserializer, integer_deserializer::IntegerDeserializer,
    list_deserializer::ListDeserializer, map_deserializer::MapDeserializer,
    null_deserializer::NullDeserializer, simple_deserializer::SimpleDeserializer,
    string_deserializer::StringDeserializer, struct_deserializer::StructDeserializer,
};

pub enum ArrayDeserializer<'a> {
    Null(NullDeserializer),
    Bool(BoolDeserializer<'a>),
    U8(IntegerDeserializer<'a, u8>),
    U16(IntegerDeserializer<'a, u16>),
    U32(IntegerDeserializer<'a, u32>),
    U64(IntegerDeserializer<'a, u64>),
    I8(IntegerDeserializer<'a, i8>),
    I16(IntegerDeserializer<'a, i16>),
    I32(IntegerDeserializer<'a, i32>),
    I64(IntegerDeserializer<'a, i64>),
    F32(FloatDeserializer<'a, f32>),
    F64(FloatDeserializer<'a, f64>),
    Date64(Date64Deserializer<'a>),
    Utf8(StringDeserializer<'a, i32>),
    LargeUtf8(StringDeserializer<'a, i64>),
    Struct(StructDeserializer<'a>),
    List(ListDeserializer<'a, i32>),
    LargeList(ListDeserializer<'a, i64>),
    Map(MapDeserializer<'a>),
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

impl<'a> From<IntegerDeserializer<'a, i8>> for ArrayDeserializer<'a> {
    fn from(value: IntegerDeserializer<'a, i8>) -> Self {
        Self::I8(value)
    }
}

impl<'a> From<IntegerDeserializer<'a, i16>> for ArrayDeserializer<'a> {
    fn from(value: IntegerDeserializer<'a, i16>) -> Self {
        Self::I16(value)
    }
}

impl<'a> From<IntegerDeserializer<'a, i32>> for ArrayDeserializer<'a> {
    fn from(value: IntegerDeserializer<'a, i32>) -> Self {
        Self::I32(value)
    }
}

impl<'a> From<IntegerDeserializer<'a, i64>> for ArrayDeserializer<'a> {
    fn from(value: IntegerDeserializer<'a, i64>) -> Self {
        Self::I64(value)
    }
}

impl<'a> From<IntegerDeserializer<'a, u8>> for ArrayDeserializer<'a> {
    fn from(value: IntegerDeserializer<'a, u8>) -> Self {
        Self::U8(value)
    }
}

impl<'a> From<IntegerDeserializer<'a, u16>> for ArrayDeserializer<'a> {
    fn from(value: IntegerDeserializer<'a, u16>) -> Self {
        Self::U16(value)
    }
}

impl<'a> From<IntegerDeserializer<'a, u32>> for ArrayDeserializer<'a> {
    fn from(value: IntegerDeserializer<'a, u32>) -> Self {
        Self::U32(value)
    }
}

impl<'a> From<IntegerDeserializer<'a, u64>> for ArrayDeserializer<'a> {
    fn from(value: IntegerDeserializer<'a, u64>) -> Self {
        Self::U64(value)
    }
}

impl<'a> From<FloatDeserializer<'a, f32>> for ArrayDeserializer<'a> {
    fn from(value: FloatDeserializer<'a, f32>) -> Self {
        Self::F32(value)
    }
}

impl<'a> From<FloatDeserializer<'a, f64>> for ArrayDeserializer<'a> {
    fn from(value: FloatDeserializer<'a, f64>) -> Self {
        Self::F64(value)
    }
}

impl<'a> From<Date64Deserializer<'a>> for ArrayDeserializer<'a> {
    fn from(value: Date64Deserializer<'a>) -> Self {
        Self::Date64(value)
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

impl<'a> From<MapDeserializer<'a>> for ArrayDeserializer<'a> {
    fn from(value: MapDeserializer<'a>) -> Self {
        Self::Map(value)
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
            $wrapper::F32($name) => $expr,
            $wrapper::F64($name) => $expr,
            $wrapper::Date64($name) => $expr,
            $wrapper::Utf8($name) => $expr,
            $wrapper::LargeUtf8($name) => $expr,
            $wrapper::Struct($name) => $expr,
            $wrapper::List($name) => $expr,
            $wrapper::LargeList($name) => $expr,
            $wrapper::Map($name) => $expr,
        }
    };
}

impl<'de> SimpleDeserializer<'de> for ArrayDeserializer<'de> {
    fn name() -> &'static str {
        "ArrayDeserializer"
    }

    fn deserialize_any<V: Visitor<'de>>(&mut self, visitor: V) -> Result<V::Value> {
        dispatch!(self, ArrayDeserializer(deser) => deser.deserialize_any(visitor))
    }

    fn deserialize_ignored_any<V: Visitor<'de>>(&mut self, visitor: V) -> Result<V::Value> {
        dispatch!(self, ArrayDeserializer(deser) => deser.deserialize_ignored_any(visitor))
    }

    fn deserialize_option<V: Visitor<'de>>(&mut self, visitor: V) -> Result<V::Value> {
        dispatch!(self, ArrayDeserializer(deser) => deser.deserialize_option(visitor))
    }

    fn deserialize_unit<V: Visitor<'de>>(&mut self, visitor: V) -> Result<V::Value> {
        dispatch!(self, ArrayDeserializer(deser) => deser.deserialize_unit(visitor))
    }

    fn deserialize_unit_struct<V: Visitor<'de>>(
        &mut self,
        name: &'static str,
        visitor: V,
    ) -> Result<V::Value> {
        dispatch!(self, ArrayDeserializer(deser) => deser.deserialize_unit_struct(name, visitor))
    }

    fn deserialize_bool<V: Visitor<'de>>(&mut self, visitor: V) -> Result<V::Value> {
        dispatch!(self, ArrayDeserializer(deser) => deser.deserialize_bool(visitor))
    }

    fn deserialize_char<V: Visitor<'de>>(&mut self, visitor: V) -> Result<V::Value> {
        dispatch!(self, ArrayDeserializer(deser) => deser.deserialize_char(visitor))
    }

    fn deserialize_u8<V: Visitor<'de>>(&mut self, visitor: V) -> Result<V::Value> {
        dispatch!(self, ArrayDeserializer(deser) => deser.deserialize_u8(visitor))
    }

    fn deserialize_u16<V: Visitor<'de>>(&mut self, visitor: V) -> Result<V::Value> {
        dispatch!(self, ArrayDeserializer(deser) => deser.deserialize_u16(visitor))
    }

    fn deserialize_u32<V: Visitor<'de>>(&mut self, visitor: V) -> Result<V::Value> {
        dispatch!(self, ArrayDeserializer(deser) => deser.deserialize_u32(visitor))
    }

    fn deserialize_u64<V: Visitor<'de>>(&mut self, visitor: V) -> Result<V::Value> {
        dispatch!(self, ArrayDeserializer(deser) => deser.deserialize_u64(visitor))
    }

    fn deserialize_i8<V: Visitor<'de>>(&mut self, visitor: V) -> Result<V::Value> {
        dispatch!(self, ArrayDeserializer(deser) => deser.deserialize_i8(visitor))
    }

    fn deserialize_i16<V: Visitor<'de>>(&mut self, visitor: V) -> Result<V::Value> {
        dispatch!(self, ArrayDeserializer(deser) => deser.deserialize_i16(visitor))
    }

    fn deserialize_i32<V: Visitor<'de>>(&mut self, visitor: V) -> Result<V::Value> {
        dispatch!(self, ArrayDeserializer(deser) => deser.deserialize_i32(visitor))
    }

    fn deserialize_i64<V: Visitor<'de>>(&mut self, visitor: V) -> Result<V::Value> {
        dispatch!(self, ArrayDeserializer(deser) => deser.deserialize_i64(visitor))
    }

    fn deserialize_f32<V: Visitor<'de>>(&mut self, visitor: V) -> Result<V::Value> {
        dispatch!(self, ArrayDeserializer(deser) => deser.deserialize_f32(visitor))
    }

    fn deserialize_f64<V: Visitor<'de>>(&mut self, visitor: V) -> Result<V::Value> {
        dispatch!(self, ArrayDeserializer(deser) => deser.deserialize_f64(visitor))
    }

    fn deserialize_str<V: Visitor<'de>>(&mut self, visitor: V) -> Result<V::Value> {
        dispatch!(self, ArrayDeserializer(deser) => deser.deserialize_str(visitor))
    }

    fn deserialize_string<V: Visitor<'de>>(&mut self, visitor: V) -> Result<V::Value> {
        dispatch!(self, ArrayDeserializer(deser) => deser.deserialize_string(visitor))
    }

    fn deserialize_struct<V: Visitor<'de>>(
        &mut self,
        name: &'static str,
        fields: &'static [&'static str],
        visitor: V,
    ) -> Result<V::Value> {
        dispatch!(self, ArrayDeserializer(deser) => deser.deserialize_struct(name, fields, visitor))
    }

    fn deserialize_map<V: Visitor<'de>>(&mut self, visitor: V) -> Result<V::Value> {
        dispatch!(self, ArrayDeserializer(deser) => deser.deserialize_map(visitor))
    }

    fn deserialize_seq<V: Visitor<'de>>(&mut self, visitor: V) -> Result<V::Value> {
        dispatch!(self, ArrayDeserializer(deser) => deser.deserialize_seq(visitor))
    }

    fn deserialize_tuple<V: Visitor<'de>>(&mut self, len: usize, visitor: V) -> Result<V::Value> {
        dispatch!(self, ArrayDeserializer(deser) => deser.deserialize_tuple(len, visitor))
    }

    fn deserialize_tuple_struct<V: Visitor<'de>>(
        &mut self,
        name: &'static str,
        len: usize,
        visitor: V,
    ) -> Result<V::Value> {
        dispatch!(self, ArrayDeserializer(deser) => deser.deserialize_tuple_struct(name, len, visitor))
    }

    fn deserialize_identifier<V: Visitor<'de>>(&mut self, visitor: V) -> Result<V::Value> {
        dispatch!(self, ArrayDeserializer(deser) => deser.deserialize_identifier(visitor))
    }

    fn deserialize_newtype_struct<V: Visitor<'de>>(
        &mut self,
        name: &'static str,
        visitor: V,
    ) -> Result<V::Value> {
        dispatch!(self, ArrayDeserializer(deser) => deser.deserialize_newtype_struct(name, visitor))
    }

    fn deserialize_enum<V: Visitor<'de>>(
        &mut self,
        name: &'static str,
        variants: &'static [&'static str],
        visitor: V,
    ) -> Result<V::Value> {
        dispatch!(self, ArrayDeserializer(deser) => deser.deserialize_enum(name, variants, visitor))
    }

    fn deserialize_bytes<V: Visitor<'de>>(&mut self, visitor: V) -> Result<V::Value> {
        dispatch!(self, ArrayDeserializer(deser) => deser.deserialize_bytes(visitor))
    }

    fn deserialize_byte_buf<V: Visitor<'de>>(&mut self, visitor: V) -> Result<V::Value> {
        dispatch!(self, ArrayDeserializer(deser) => deser.deserialize_byte_buf(visitor))
    }
}
