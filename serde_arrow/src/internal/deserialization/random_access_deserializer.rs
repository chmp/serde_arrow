//! An extension of the serde interface that allows to deserialize a value at a given index
//!

use serde::{de::Visitor, Deserializer};

use crate::internal::error::{fail, try_, Context, ContextSupport, Error, Result};

// NOTE: the lifetime of the reference is not relevant for the lifetime of the deserialized, only
// the lifetime of the contained arrays
pub struct PositionedDeserializer<'this, D>(pub &'this D, pub usize);

#[allow(unused)]
pub trait RandomAccessDeserializer<'de>: Context + Sized {
    fn at(&self, idx: usize) -> PositionedDeserializer<'_, Self> {
        PositionedDeserializer(self, idx)
    }

    fn is_some(&self, idx: usize) -> Result<bool> {
        fail!(in self, "Deserializer does not implement is_some_at")
    }

    fn deserialize_any_some<V: Visitor<'de>>(&self, visitor: V, idx: usize) -> Result<V::Value> {
        fail!(in self, "Deserializer does not implement deserialize_any_some_at");
    }

    fn deserialize_any<V: Visitor<'de>>(&self, visitor: V, idx: usize) -> Result<V::Value> {
        try_(|| {
            if self.is_some(idx)? {
                self.deserialize_any_some(visitor, idx)
            } else {
                visitor.visit_none()
            }
        })
        .ctx(self)
    }

    fn deserialize_option<V: Visitor<'de>>(&self, visitor: V, idx: usize) -> Result<V::Value> {
        try_(|| {
            if self.is_some(idx)? {
                visitor.visit_some(self.at(idx))
            } else {
                visitor.visit_none()
            }
        })
        .ctx(self)
    }

    fn deserialize_ignored_any<V: Visitor<'de>>(&self, visitor: V, idx: usize) -> Result<V::Value> {
        self.deserialize_any(visitor, idx)
    }

    fn deserialize_bool<V: Visitor<'de>>(&self, visitor: V, idx: usize) -> Result<V::Value> {
        fail!(in self, "Deserializer does not implement deserialize_bool_at");
    }

    fn deserialize_i8<V: Visitor<'de>>(&self, visitor: V, idx: usize) -> Result<V::Value> {
        fail!(in self, "Deserializer does not implement deserialize_i8_at");
    }

    fn deserialize_i16<V: Visitor<'de>>(&self, visitor: V, idx: usize) -> Result<V::Value> {
        fail!(in self, "Deserializer does not implement deserialize_i16_at");
    }

    fn deserialize_i32<V: Visitor<'de>>(&self, visitor: V, idx: usize) -> Result<V::Value> {
        fail!(in self, "Deserializer does not implement deserialize_i32_at");
    }

    fn deserialize_i64<V: Visitor<'de>>(&self, visitor: V, idx: usize) -> Result<V::Value> {
        fail!(in self, "Deserializer does not implement deserialize_i64_at");
    }

    fn deserialize_u8<V: Visitor<'de>>(&self, visitor: V, idx: usize) -> Result<V::Value> {
        fail!(in self, "Deserializer does not implement deserialize_u8_at");
    }

    fn deserialize_u16<V: Visitor<'de>>(&self, visitor: V, idx: usize) -> Result<V::Value> {
        fail!(in self, "Deserializer does not implement deserialize_u16_at");
    }

    fn deserialize_u32<V: Visitor<'de>>(&self, visitor: V, idx: usize) -> Result<V::Value> {
        fail!(in self, "Deserializer does not implement deserialize_u32_at");
    }

    fn deserialize_u64<V: Visitor<'de>>(&self, visitor: V, idx: usize) -> Result<V::Value> {
        fail!(in self, "Deserializer does not implement deserialize_u64_at");
    }

    fn deserialize_f32<V: Visitor<'de>>(&self, visitor: V, idx: usize) -> Result<V::Value> {
        fail!(in self, "Deserializer does not implement deserialize_f32_at");
    }

    fn deserialize_f64<V: Visitor<'de>>(&self, visitor: V, idx: usize) -> Result<V::Value> {
        fail!(in self, "Deserializer does not implement deserialize_f64_at");
    }

    fn deserialize_char<V: Visitor<'de>>(&self, visitor: V, idx: usize) -> Result<V::Value> {
        fail!(in self, "Deserializer does not implement deserialize_char_at");
    }

    fn deserialize_str<V: Visitor<'de>>(&self, visitor: V, idx: usize) -> Result<V::Value> {
        fail!(in self, "Deserializer does not implement deserialize_str_at");
    }

    fn deserialize_string<V: Visitor<'de>>(&self, visitor: V, idx: usize) -> Result<V::Value> {
        fail!(in self, "Deserializer does not implement deserialize_string_at");
    }

    fn deserialize_map<V: Visitor<'de>>(&self, visitor: V, idx: usize) -> Result<V::Value> {
        fail!(in self, "Deserializer does not implement deserialize_map_at");
    }

    fn deserialize_struct<V: Visitor<'de>>(
        &self,
        name: &'static str,
        fields: &'static [&'static str],
        visitor: V,
        idx: usize,
    ) -> Result<V::Value> {
        fail!(in self, "Deserializer does not implement deserialize_struct_at");
    }

    fn deserialize_byte_buf<V: Visitor<'de>>(&self, visitor: V, idx: usize) -> Result<V::Value> {
        fail!(in self, "Deserializer does not implement deserialize_byte_buf_at");
    }

    fn deserialize_bytes<V: Visitor<'de>>(&self, visitor: V, idx: usize) -> Result<V::Value> {
        fail!(in self, "Deserializer does not implement deserialize_bytes_at");
    }

    fn deserialize_enum<V: Visitor<'de>>(
        &self,
        name: &'static str,
        variants: &'static [&'static str],
        visitor: V,
        idx: usize,
    ) -> Result<V::Value> {
        fail!(in self, "Deserializer does not implement deserialize_enum_at");
    }

    fn deserialize_identifier<V: Visitor<'de>>(&self, visitor: V, idx: usize) -> Result<V::Value> {
        fail!(in self, "Deserializer does not implement deserialize_identifier_at");
    }

    fn deserialize_newtype_struct<V: Visitor<'de>>(
        &self,
        name: &'static str,
        visitor: V,
        idx: usize,
    ) -> Result<V::Value> {
        visitor.visit_newtype_struct(PositionedDeserializer(self, idx))
    }

    fn deserialize_tuple<V: Visitor<'de>>(
        &self,
        len: usize,
        visitor: V,
        idx: usize,
    ) -> Result<V::Value> {
        fail!(in self, "Deserializer does not implement deserialize_tuple_at");
    }

    fn deserialize_seq<V: Visitor<'de>>(&self, visitor: V, idx: usize) -> Result<V::Value> {
        fail!(in self, "Deserializer does not implement deserialize_seq_at");
    }

    fn deserialize_tuple_struct<V: Visitor<'de>>(
        &self,
        name: &'static str,
        len: usize,
        visitor: V,
        idx: usize,
    ) -> Result<V::Value> {
        fail!(in self,
            "Deserializer does not implement deserialize_tuple_struct_at",
        );
    }

    fn deserialize_unit<V: Visitor<'de>>(&self, visitor: V, idx: usize) -> Result<V::Value> {
        fail!(in self, "Deserializer does not implement deserialize_unit_at");
    }

    fn deserialize_unit_struct<V: Visitor<'de>>(
        &self,
        name: &'static str,
        visitor: V,
        idx: usize,
    ) -> Result<V::Value> {
        fail!(in self,
            "Deserializer does not implement deserialize_unit_struct_at",
        );
    }
}

impl<'de, D: RandomAccessDeserializer<'de>> Deserializer<'de> for PositionedDeserializer<'_, D> {
    type Error = Error;

    fn deserialize_any<V: Visitor<'de>>(self, visitor: V) -> Result<V::Value> {
        self.0.deserialize_any(visitor, self.1)
    }

    fn deserialize_ignored_any<V: Visitor<'de>>(self, visitor: V) -> Result<V::Value> {
        self.0.deserialize_ignored_any(visitor, self.1)
    }

    fn deserialize_bool<V: Visitor<'de>>(self, visitor: V) -> Result<V::Value> {
        self.0.deserialize_bool(visitor, self.1)
    }

    fn deserialize_i8<V: Visitor<'de>>(self, visitor: V) -> Result<V::Value> {
        self.0.deserialize_i8(visitor, self.1)
    }

    fn deserialize_i16<V: Visitor<'de>>(self, visitor: V) -> Result<V::Value> {
        self.0.deserialize_i16(visitor, self.1)
    }

    fn deserialize_i32<V: Visitor<'de>>(self, visitor: V) -> Result<V::Value> {
        self.0.deserialize_i32(visitor, self.1)
    }

    fn deserialize_i64<V: Visitor<'de>>(self, visitor: V) -> Result<V::Value> {
        self.0.deserialize_i64(visitor, self.1)
    }

    fn deserialize_u8<V: Visitor<'de>>(self, visitor: V) -> Result<V::Value> {
        self.0.deserialize_u8(visitor, self.1)
    }

    fn deserialize_u16<V: Visitor<'de>>(self, visitor: V) -> Result<V::Value> {
        self.0.deserialize_u16(visitor, self.1)
    }

    fn deserialize_u32<V: Visitor<'de>>(self, visitor: V) -> Result<V::Value> {
        self.0.deserialize_u32(visitor, self.1)
    }

    fn deserialize_u64<V: Visitor<'de>>(self, visitor: V) -> Result<V::Value> {
        self.0.deserialize_u64(visitor, self.1)
    }

    fn deserialize_f32<V: Visitor<'de>>(self, visitor: V) -> Result<V::Value> {
        self.0.deserialize_f32(visitor, self.1)
    }

    fn deserialize_f64<V: Visitor<'de>>(self, visitor: V) -> Result<V::Value> {
        self.0.deserialize_f64(visitor, self.1)
    }

    fn deserialize_char<V: Visitor<'de>>(self, visitor: V) -> Result<V::Value> {
        self.0.deserialize_char(visitor, self.1)
    }

    fn deserialize_str<V: Visitor<'de>>(self, visitor: V) -> Result<V::Value> {
        self.0.deserialize_str(visitor, self.1)
    }

    fn deserialize_string<V: Visitor<'de>>(self, visitor: V) -> Result<V::Value> {
        self.0.deserialize_string(visitor, self.1)
    }

    fn deserialize_map<V: Visitor<'de>>(self, visitor: V) -> Result<V::Value> {
        self.0.deserialize_map(visitor, self.1)
    }

    fn deserialize_struct<V: Visitor<'de>>(
        self,
        name: &'static str,
        fields: &'static [&'static str],
        visitor: V,
    ) -> Result<V::Value> {
        self.0.deserialize_struct(name, fields, visitor, self.1)
    }

    fn deserialize_byte_buf<V: Visitor<'de>>(self, visitor: V) -> Result<V::Value> {
        self.0.deserialize_byte_buf(visitor, self.1)
    }

    fn deserialize_bytes<V: Visitor<'de>>(self, visitor: V) -> Result<V::Value> {
        self.0.deserialize_bytes(visitor, self.1)
    }

    fn deserialize_enum<V: Visitor<'de>>(
        self,
        name: &'static str,
        variants: &'static [&'static str],
        visitor: V,
    ) -> Result<V::Value> {
        self.0.deserialize_enum(name, variants, visitor, self.1)
    }

    fn deserialize_identifier<V: Visitor<'de>>(self, visitor: V) -> Result<V::Value> {
        self.0.deserialize_identifier(visitor, self.1)
    }

    fn deserialize_option<V: Visitor<'de>>(self, visitor: V) -> Result<V::Value> {
        self.0.deserialize_option(visitor, self.1)
    }

    fn deserialize_newtype_struct<V: Visitor<'de>>(
        self,
        name: &'static str,
        visitor: V,
    ) -> Result<V::Value> {
        self.0.deserialize_newtype_struct(name, visitor, self.1)
    }

    fn deserialize_tuple<V: Visitor<'de>>(self, len: usize, visitor: V) -> Result<V::Value> {
        self.0.deserialize_tuple(len, visitor, self.1)
    }

    fn deserialize_seq<V: Visitor<'de>>(self, visitor: V) -> Result<V::Value> {
        self.0.deserialize_seq(visitor, self.1)
    }

    fn deserialize_tuple_struct<V: Visitor<'de>>(
        self,
        name: &'static str,
        len: usize,
        visitor: V,
    ) -> Result<V::Value> {
        self.0.deserialize_tuple_struct(name, len, visitor, self.1)
    }

    fn deserialize_unit<V: Visitor<'de>>(self, visitor: V) -> Result<V::Value> {
        self.0.deserialize_unit(visitor, self.1)
    }

    fn deserialize_unit_struct<V: Visitor<'de>>(
        self,
        name: &'static str,
        visitor: V,
    ) -> Result<V::Value> {
        self.0.deserialize_unit_struct(name, visitor, self.1)
    }
}
