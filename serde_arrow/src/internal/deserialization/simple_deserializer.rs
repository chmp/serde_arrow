use serde::{de::Visitor, Deserializer};

use crate::internal::{
    error::{fail, Error, Result},
    utils::Mut,
};

#[allow(unused)]
pub trait SimpleDeserializer<'de>: Sized {
    fn name() -> &'static str;

    fn deserialize_any<V: Visitor<'de>>(&mut self, visitor: V) -> Result<V::Value> {
        fail!("{} does not implement deserialize_any", Self::name());
    }

    fn deserialize_ignored_any<V: Visitor<'de>>(&mut self, visitor: V) -> Result<V::Value> {
        self.deserialize_any(visitor)
    }

    fn deserialize_bool<V: Visitor<'de>>(&mut self, visitor: V) -> Result<V::Value> {
        fail!("{} does not implement deserialize_bool", Self::name());
    }

    fn deserialize_i8<V: Visitor<'de>>(&mut self, visitor: V) -> Result<V::Value> {
        fail!("{} does not implement deserialize_i8", Self::name());
    }

    fn deserialize_i16<V: Visitor<'de>>(&mut self, visitor: V) -> Result<V::Value> {
        fail!("{} does not implement deserialize_i16", Self::name());
    }

    fn deserialize_i32<V: Visitor<'de>>(&mut self, visitor: V) -> Result<V::Value> {
        fail!("{} does not implement deserialize_i32", Self::name());
    }

    fn deserialize_i64<V: Visitor<'de>>(&mut self, visitor: V) -> Result<V::Value> {
        fail!("{} does not implement deserialize_i64", Self::name());
    }

    fn deserialize_u8<V: Visitor<'de>>(&mut self, visitor: V) -> Result<V::Value> {
        fail!("{} does not implement deserialize_u8", Self::name());
    }

    fn deserialize_u16<V: Visitor<'de>>(&mut self, visitor: V) -> Result<V::Value> {
        fail!("{} does not implement deserialize_u16", Self::name());
    }

    fn deserialize_u32<V: Visitor<'de>>(&mut self, visitor: V) -> Result<V::Value> {
        fail!("{} does not implement deserialize_u32", Self::name());
    }

    fn deserialize_u64<V: Visitor<'de>>(&mut self, visitor: V) -> Result<V::Value> {
        fail!("{} does not implement deserialize_u64", Self::name());
    }

    fn deserialize_f32<V: Visitor<'de>>(&mut self, visitor: V) -> Result<V::Value> {
        fail!("{} does not implement deserialize_f32", Self::name());
    }

    fn deserialize_f64<V: Visitor<'de>>(&mut self, visitor: V) -> Result<V::Value> {
        fail!("{} does not implement deserialize_f64", Self::name());
    }

    fn deserialize_char<V: Visitor<'de>>(&mut self, visitor: V) -> Result<V::Value> {
        fail!("{} does not implement deserialize_char", Self::name());
    }

    fn deserialize_str<V: Visitor<'de>>(&mut self, visitor: V) -> Result<V::Value> {
        fail!("{} does not implement deserialize_str", Self::name());
    }

    fn deserialize_string<V: Visitor<'de>>(&mut self, visitor: V) -> Result<V::Value> {
        fail!("{} does not implement deserialize_string", Self::name());
    }

    fn deserialize_map<V: Visitor<'de>>(&mut self, visitor: V) -> Result<V::Value> {
        fail!("{} does not implement deserialize_map", Self::name());
    }

    fn deserialize_struct<V: Visitor<'de>>(
        &mut self,
        name: &'static str,
        fields: &'static [&'static str],
        visitor: V,
    ) -> Result<V::Value> {
        fail!("{} does not implement deserialize_struct", Self::name());
    }

    fn deserialize_byte_buf<V: Visitor<'de>>(&mut self, visitor: V) -> Result<V::Value> {
        fail!("{} does not implement deserialize_byte_buf", Self::name());
    }

    fn deserialize_bytes<V: Visitor<'de>>(&mut self, visitor: V) -> Result<V::Value> {
        fail!("{} does not implement deserialize_bytes", Self::name());
    }

    fn deserialize_enum<V: Visitor<'de>>(
        &mut self,
        name: &'static str,
        variants: &'static [&'static str],
        visitor: V,
    ) -> Result<V::Value> {
        fail!("{} does not implement deserialize_enum", Self::name());
    }

    fn deserialize_identifier<V: Visitor<'de>>(&mut self, visitor: V) -> Result<V::Value> {
        fail!("{} does not implement deserialize_identifier", Self::name());
    }

    fn deserialize_option<V: Visitor<'de>>(&mut self, visitor: V) -> Result<V::Value> {
        fail!("{} does not implement deserialize_option", Self::name());
    }

    fn deserialize_newtype_struct<V: Visitor<'de>>(
        &mut self,
        name: &'static str,
        visitor: V,
    ) -> Result<V::Value> {
        visitor.visit_newtype_struct(Mut(self))
    }

    fn deserialize_tuple<V: Visitor<'de>>(&mut self, len: usize, visitor: V) -> Result<V::Value> {
        fail!("{} does not implement deserialize_tuple", Self::name());
    }

    fn deserialize_seq<V: Visitor<'de>>(&mut self, visitor: V) -> Result<V::Value> {
        fail!("{} does not implement deserialize_seq", Self::name());
    }

    fn deserialize_tuple_struct<V: Visitor<'de>>(
        &mut self,
        name: &'static str,
        len: usize,
        visitor: V,
    ) -> Result<V::Value> {
        fail!(
            "{} does not implement deserialize_tuple_struct",
            Self::name()
        );
    }

    fn deserialize_unit<V: Visitor<'de>>(&mut self, visitor: V) -> Result<V::Value> {
        fail!("{} does not implement deserialize_unit", Self::name());
    }

    fn deserialize_unit_struct<V: Visitor<'de>>(
        &mut self,
        name: &'static str,
        visitor: V,
    ) -> Result<V::Value> {
        fail!(
            "{} does not implement deserialize_unit_struct",
            Self::name()
        );
    }
}

impl<'a, 'de, D: SimpleDeserializer<'de>> Deserializer<'de> for Mut<'a, D> {
    type Error = Error;

    fn deserialize_any<V: Visitor<'de>>(self, visitor: V) -> Result<V::Value> {
        self.0.deserialize_any(visitor)
    }

    fn deserialize_ignored_any<V: Visitor<'de>>(self, visitor: V) -> Result<V::Value> {
        self.0.deserialize_ignored_any(visitor)
    }

    fn deserialize_bool<V: Visitor<'de>>(self, visitor: V) -> Result<V::Value> {
        self.0.deserialize_bool(visitor)
    }

    fn deserialize_i8<V: Visitor<'de>>(self, visitor: V) -> Result<V::Value> {
        self.0.deserialize_i8(visitor)
    }

    fn deserialize_i16<V: Visitor<'de>>(self, visitor: V) -> Result<V::Value> {
        self.0.deserialize_i16(visitor)
    }

    fn deserialize_i32<V: Visitor<'de>>(self, visitor: V) -> Result<V::Value> {
        self.0.deserialize_i32(visitor)
    }

    fn deserialize_i64<V: Visitor<'de>>(self, visitor: V) -> Result<V::Value> {
        self.0.deserialize_i64(visitor)
    }

    fn deserialize_u8<V: Visitor<'de>>(self, visitor: V) -> Result<V::Value> {
        self.0.deserialize_u8(visitor)
    }

    fn deserialize_u16<V: Visitor<'de>>(self, visitor: V) -> Result<V::Value> {
        self.0.deserialize_u16(visitor)
    }

    fn deserialize_u32<V: Visitor<'de>>(self, visitor: V) -> Result<V::Value> {
        self.0.deserialize_u32(visitor)
    }

    fn deserialize_u64<V: Visitor<'de>>(self, visitor: V) -> Result<V::Value> {
        self.0.deserialize_u64(visitor)
    }

    fn deserialize_f32<V: Visitor<'de>>(self, visitor: V) -> Result<V::Value> {
        self.0.deserialize_f32(visitor)
    }

    fn deserialize_f64<V: Visitor<'de>>(self, visitor: V) -> Result<V::Value> {
        self.0.deserialize_f64(visitor)
    }

    fn deserialize_char<V: Visitor<'de>>(self, visitor: V) -> Result<V::Value> {
        self.0.deserialize_char(visitor)
    }

    fn deserialize_str<V: Visitor<'de>>(self, visitor: V) -> Result<V::Value> {
        self.0.deserialize_str(visitor)
    }

    fn deserialize_string<V: Visitor<'de>>(self, visitor: V) -> Result<V::Value> {
        self.0.deserialize_string(visitor)
    }

    fn deserialize_map<V: Visitor<'de>>(self, visitor: V) -> Result<V::Value> {
        self.0.deserialize_map(visitor)
    }

    fn deserialize_struct<V: Visitor<'de>>(
        self,
        name: &'static str,
        fields: &'static [&'static str],
        visitor: V,
    ) -> Result<V::Value> {
        self.0.deserialize_struct(name, fields, visitor)
    }

    fn deserialize_byte_buf<V: Visitor<'de>>(self, visitor: V) -> Result<V::Value> {
        self.0.deserialize_byte_buf(visitor)
    }

    fn deserialize_bytes<V: Visitor<'de>>(self, visitor: V) -> Result<V::Value> {
        self.0.deserialize_bytes(visitor)
    }

    fn deserialize_enum<V: Visitor<'de>>(
        self,
        name: &'static str,
        variants: &'static [&'static str],
        visitor: V,
    ) -> Result<V::Value> {
        self.0.deserialize_enum(name, variants, visitor)
    }

    fn deserialize_identifier<V: Visitor<'de>>(self, visitor: V) -> Result<V::Value> {
        self.0.deserialize_identifier(visitor)
    }

    fn deserialize_option<V: Visitor<'de>>(self, visitor: V) -> Result<V::Value> {
        self.0.deserialize_option(visitor)
    }

    fn deserialize_newtype_struct<V: Visitor<'de>>(
        self,
        name: &'static str,
        visitor: V,
    ) -> Result<V::Value> {
        self.0.deserialize_newtype_struct(name, visitor)
    }

    fn deserialize_tuple<V: Visitor<'de>>(self, len: usize, visitor: V) -> Result<V::Value> {
        self.0.deserialize_tuple(len, visitor)
    }

    fn deserialize_seq<V: Visitor<'de>>(self, visitor: V) -> Result<V::Value> {
        self.0.deserialize_seq(visitor)
    }

    fn deserialize_tuple_struct<V: Visitor<'de>>(
        self,
        name: &'static str,
        len: usize,
        visitor: V,
    ) -> Result<V::Value> {
        self.0.deserialize_tuple_struct(name, len, visitor)
    }

    fn deserialize_unit<V: Visitor<'de>>(self, visitor: V) -> Result<V::Value> {
        self.0.deserialize_unit(visitor)
    }

    fn deserialize_unit_struct<V: Visitor<'de>>(
        self,
        name: &'static str,
        visitor: V,
    ) -> Result<V::Value> {
        self.0.deserialize_unit_struct(name, visitor)
    }
}
