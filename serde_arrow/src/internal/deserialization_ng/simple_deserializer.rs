use serde::Deserializer;

use crate::{internal::{error::fail, serialization_ng::utils::Mut}, Error, Result};


#[allow(unused)]
pub trait SimpleDeserializer<'de> {
    fn name() -> &'static str;

    fn deserialize_any<V: serde::de::Visitor<'de>>(&mut self, visitor: V) -> Result<V::Value> {
        fail!("{} does not implement deserialize_any", Self::name());
    }

    fn deserialize_ignored_any<V: serde::de::Visitor<'de>>(&mut self, visitor: V) -> Result<V::Value> {
        fail!("{} does not implement deserialize_ignored_any", Self::name());
    }

    fn deserialize_bool<V: serde::de::Visitor<'de>>(&mut self, visitor: V) -> Result<V::Value> {
        fail!("{} does not implement deserialize_bool", Self::name());
    }

    fn deserialize_i8<V: serde::de::Visitor<'de>>(&mut self, visitor: V) -> Result<V::Value> {
        fail!("{} does not implement deserialize_i8", Self::name());
    }

    fn deserialize_i16<V: serde::de::Visitor<'de>>(&mut self, visitor: V) -> Result<V::Value> {
        fail!("{} does not implement deserialize_i16", Self::name());
    }

    fn deserialize_i32<V: serde::de::Visitor<'de>>(&mut self, visitor: V) -> Result<V::Value> {
        fail!("{} does not implement deserialize_i32", Self::name());
    }

    fn deserialize_i64<V: serde::de::Visitor<'de>>(&mut self, visitor: V) -> Result<V::Value> {
        fail!("{} does not implement deserialize_i64", Self::name());
    }

    fn deserialize_u8<V: serde::de::Visitor<'de>>(&mut self, visitor: V) -> Result<V::Value> {
        fail!("{} does not implement deserialize_u8", Self::name());
    }

    fn deserialize_u16<V: serde::de::Visitor<'de>>(&mut self, visitor: V) -> Result<V::Value> {
        fail!("{} does not implement deserialize_u16", Self::name());
    }

    fn deserialize_u32<V: serde::de::Visitor<'de>>(&mut self, visitor: V) -> Result<V::Value> {
        fail!("{} does not implement deserialize_u32", Self::name());
    }

    fn deserialize_u64<V: serde::de::Visitor<'de>>(&mut self, visitor: V) -> Result<V::Value> {
        fail!("{} does not implement deserialize_u64", Self::name());
    }

    fn deserialize_f32<V: serde::de::Visitor<'de>>(&mut self, visitor: V) -> Result<V::Value> {
        fail!("{} does not implement deserialize_f32", Self::name());
    }

    fn deserialize_f64<V: serde::de::Visitor<'de>>(&mut self, visitor: V) -> Result<V::Value> {
        fail!("{} does not implement deserialize_f64", Self::name());
    }

    fn deserialize_char<V: serde::de::Visitor<'de>>(&mut self, visitor: V) -> Result<V::Value> {
        fail!("{} does not implement deserialize_char", Self::name());
    }

    fn deserialize_str<V: serde::de::Visitor<'de>>(&mut self, visitor: V) -> Result<V::Value> {
        fail!("{} does not implement deserialize_str", Self::name());
    }

    fn deserialize_string<V: serde::de::Visitor<'de>>(&mut self, visitor: V) -> Result<V::Value> {
        fail!("{} does not implement deserialize_string", Self::name());
    }

    fn deserialize_map<V: serde::de::Visitor<'de>>(&mut self, visitor: V) -> Result<V::Value> {
        fail!("{} does not implement deserialize_map", Self::name());
    }

    fn deserialize_struct<V: serde::de::Visitor<'de>>(
        &mut self,
        name: &'static str,
        fields: &'static [&'static str],
        visitor: V,
    ) -> Result<V::Value> {
        fail!("{} does not implement deserialize_struct", Self::name());
    }

    fn deserialize_byte_buf<V: serde::de::Visitor<'de>>(&mut self, visitor: V) -> Result<V::Value> {
        fail!("{} does not implement deserialize_byte_buf", Self::name());
    }

    fn deserialize_bytes<V: serde::de::Visitor<'de>>(&mut self, visitor: V) -> Result<V::Value> {
        fail!("{} does not implement deserialize_bytes", Self::name());
    }

    fn deserialize_enum<V: serde::de::Visitor<'de>>(
        &mut self,
        name: &'static str,
        variants: &'static [&'static str],
        visitor: V,
    ) -> Result<V::Value> {
        fail!("{} does not implement deserialize_enum", Self::name());
    }

    fn deserialize_identifier<V: serde::de::Visitor<'de>>(&mut self, visitor: V) -> Result<V::Value> {
        fail!("{} does not implement deserialize_identifier", Self::name());
    }

    fn deserialize_option<V: serde::de::Visitor<'de>>(&mut self, visitor: V) -> Result<V::Value> {
        fail!("{} does not implement deserialize_option", Self::name());
    }

    fn deserialize_newtype_struct<V: serde::de::Visitor<'de>>(
        &mut self,
        name: &'static str,
        visitor: V,
    ) -> Result<V::Value> {
        fail!("{} does not implement deserialize_newtype_struct", Self::name());
    }

    fn deserialize_tuple<V: serde::de::Visitor<'de>>(
        &mut self,
        len: usize,
        visitor: V,
    ) -> Result<V::Value> {
        fail!("{} does not implement deserialize_tuple", Self::name());
    }

    fn deserialize_seq<V: serde::de::Visitor<'de>>(&mut self, visitor: V) -> Result<V::Value> {
        fail!("{} does not implement deserialize_seq", Self::name());
    }

    fn deserialize_tuple_struct<V: serde::de::Visitor<'de>>(
        &mut self,
        name: &'static str,
        len: usize,
        visitor: V,
    ) -> Result<V::Value> {
        fail!("{} does not implement deserialize_tuple_struct", Self::name());
    }

    fn deserialize_unit<V: serde::de::Visitor<'de>>(&mut self, visitor: V) -> Result<V::Value> {
        fail!("{} does not implement deserialize_unit", Self::name());
    }

    fn deserialize_unit_struct<V: serde::de::Visitor<'de>>(
        &mut self,
        name: &'static str,
        visitor: V,
    ) -> Result<V::Value> {
        fail!("{} does not implement deserialize_unit_struct", Self::name());
    }

}

impl<'a, 'de, D: SimpleDeserializer<'de>> Deserializer<'de> for Mut<'a, D> {
    type Error = Error;

    fn deserialize_any<V: serde::de::Visitor<'de>>(self, visitor: V) -> Result<V::Value> {
        self.0.deserialize_any(visitor)
    }

    fn deserialize_ignored_any<V: serde::de::Visitor<'de>>(self, visitor: V) -> Result<V::Value> {
        self.0.deserialize_ignored_any(visitor)
    }

    fn deserialize_bool<V: serde::de::Visitor<'de>>(self, visitor: V) -> Result<V::Value> {
        self.0.deserialize_bool(visitor)
    }

    fn deserialize_i8<V: serde::de::Visitor<'de>>(self, visitor: V) -> Result<V::Value> {
        self.0.deserialize_i8(visitor)
    }

    fn deserialize_i16<V: serde::de::Visitor<'de>>(self, visitor: V) -> Result<V::Value> {
        self.0.deserialize_i16(visitor)
    }

    fn deserialize_i32<V: serde::de::Visitor<'de>>(self, visitor: V) -> Result<V::Value> {
        self.0.deserialize_i32(visitor)
    }

    fn deserialize_i64<V: serde::de::Visitor<'de>>(self, visitor: V) -> Result<V::Value> {
        self.0.deserialize_i64(visitor)
    }

    fn deserialize_u8<V: serde::de::Visitor<'de>>(self, visitor: V) -> Result<V::Value> {
        self.0.deserialize_u8(visitor)
    }

    fn deserialize_u16<V: serde::de::Visitor<'de>>(self, visitor: V) -> Result<V::Value> {
        self.0.deserialize_u16(visitor)
    }

    fn deserialize_u32<V: serde::de::Visitor<'de>>(self, visitor: V) -> Result<V::Value> {
        self.0.deserialize_u32(visitor)
    }

    fn deserialize_u64<V: serde::de::Visitor<'de>>(self, visitor: V) -> Result<V::Value> {
        self.0.deserialize_u64(visitor)
    }

    fn deserialize_f32<V: serde::de::Visitor<'de>>(self, visitor: V) -> Result<V::Value> {
        self.0.deserialize_f32(visitor)
    }

    fn deserialize_f64<V: serde::de::Visitor<'de>>(self, visitor: V) -> Result<V::Value> {
        self.0.deserialize_f64(visitor)
    }

    fn deserialize_char<V: serde::de::Visitor<'de>>(self, visitor: V) -> Result<V::Value> {
        self.0.deserialize_char(visitor)
    }

    fn deserialize_str<V: serde::de::Visitor<'de>>(self, visitor: V) -> Result<V::Value> {
        self.0.deserialize_str(visitor)
    }

    fn deserialize_string<V: serde::de::Visitor<'de>>(self, visitor: V) -> Result<V::Value> {
        self.0.deserialize_string(visitor)
    }

    fn deserialize_map<V: serde::de::Visitor<'de>>(self, visitor: V) -> Result<V::Value> {
        self.0.deserialize_map(visitor)
    }

    fn deserialize_struct<V: serde::de::Visitor<'de>>(
        self,
        name: &'static str,
        fields: &'static [&'static str],
        visitor: V,
    ) -> Result<V::Value> {
        self.0.deserialize_struct(name, fields, visitor)
    }

    fn deserialize_byte_buf<V: serde::de::Visitor<'de>>(self, visitor: V) -> Result<V::Value> {
        self.0.deserialize_byte_buf(visitor)
    }

    fn deserialize_bytes<V: serde::de::Visitor<'de>>(self, visitor: V) -> Result<V::Value> {
        self.0.deserialize_bytes(visitor)
    }

    fn deserialize_enum<V: serde::de::Visitor<'de>>(
        self,
        name: &'static str,
        variants: &'static [&'static str],
        visitor: V,
    ) -> Result<V::Value> {
        self.0.deserialize_enum(name, variants, visitor)
    }

    fn deserialize_identifier<V: serde::de::Visitor<'de>>(self, visitor: V) -> Result<V::Value> {
        self.0.deserialize_identifier(visitor)
    }

    fn deserialize_option<V: serde::de::Visitor<'de>>(self, visitor: V) -> Result<V::Value> {
        self.0.deserialize_option(visitor)
    }

    fn deserialize_newtype_struct<V: serde::de::Visitor<'de>>(
        self,
        name: &'static str,
        visitor: V,
    ) -> Result<V::Value> {
        self.0.deserialize_newtype_struct(name, visitor)
    }

    fn deserialize_tuple<V: serde::de::Visitor<'de>>(
        self,
        len: usize,
        visitor: V,
    ) -> Result<V::Value> {
        self.0.deserialize_tuple(len, visitor)
    }

    fn deserialize_seq<V: serde::de::Visitor<'de>>(self, visitor: V) -> Result<V::Value> {
        self.0.deserialize_seq(visitor)
    }

    fn deserialize_tuple_struct<V: serde::de::Visitor<'de>>(
        self,
        name: &'static str,
        len: usize,
        visitor: V,
    ) -> Result<V::Value> {
        self.0.deserialize_tuple_struct(name, len, visitor)
    }

    fn deserialize_unit<V: serde::de::Visitor<'de>>(self, visitor: V) -> Result<V::Value> {
        self.0.deserialize_unit(visitor)
    }

    fn deserialize_unit_struct<V: serde::de::Visitor<'de>>(
        self,
        name: &'static str,
        visitor: V,
    ) -> Result<V::Value> {
        self.0.deserialize_unit_struct(name, visitor)
    }
}

