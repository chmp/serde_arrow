//! An extension of the serde interface that allows to deserialize a value at a given index
//!

use serde::{de::Visitor, Deserializer};

use crate::internal::error::{fail, try_, Context, ContextSupport, Error, Result};

// NOTE: the lifetime of the reference is not relevant for the lifetime of the deserialized, only
// the lifetime of the contained arrays
pub struct PositionedDeserializer<'this, D>(&'this D, usize);

#[allow(unused)]
pub trait RandomAccessDeserializer<'de>: Context + Sized {
    fn at(&self, idx: usize) -> PositionedDeserializer<'_, Self> {
        PositionedDeserializer(self, idx)
    }

    fn is_some(&self, idx: usize) -> Result<bool>;

    fn deserialize_any_some<V: Visitor<'de>>(&self, visitor: V, idx: usize) -> Result<V::Value> {
        fail!(in self, "Deserializer does not implement deserialize_any");
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
        fail!(in self, "Deserializer does not implement deserialize_bool");
    }

    fn deserialize_i8<V: Visitor<'de>>(&self, visitor: V, idx: usize) -> Result<V::Value> {
        fail!(in self, "Deserializer does not implement deserialize_i8");
    }

    fn deserialize_i16<V: Visitor<'de>>(&self, visitor: V, idx: usize) -> Result<V::Value> {
        fail!(in self, "Deserializer does not implement deserialize_i16");
    }

    fn deserialize_i32<V: Visitor<'de>>(&self, visitor: V, idx: usize) -> Result<V::Value> {
        fail!(in self, "Deserializer does not implement deserialize_i32");
    }

    fn deserialize_i64<V: Visitor<'de>>(&self, visitor: V, idx: usize) -> Result<V::Value> {
        fail!(in self, "Deserializer does not implement deserialize_i64");
    }

    fn deserialize_u8<V: Visitor<'de>>(&self, visitor: V, idx: usize) -> Result<V::Value> {
        fail!(in self, "Deserializer does not implement deserialize_u8");
    }

    fn deserialize_u16<V: Visitor<'de>>(&self, visitor: V, idx: usize) -> Result<V::Value> {
        fail!(in self, "Deserializer does not implement deserialize_u16");
    }

    fn deserialize_u32<V: Visitor<'de>>(&self, visitor: V, idx: usize) -> Result<V::Value> {
        fail!(in self, "Deserializer does not implement deserialize_u32");
    }

    fn deserialize_u64<V: Visitor<'de>>(&self, visitor: V, idx: usize) -> Result<V::Value> {
        fail!(in self, "Deserializer does not implement deserialize_u64");
    }

    fn deserialize_f32<V: Visitor<'de>>(&self, visitor: V, idx: usize) -> Result<V::Value> {
        fail!(in self, "Deserializer does not implement deserialize_f32");
    }

    fn deserialize_f64<V: Visitor<'de>>(&self, visitor: V, idx: usize) -> Result<V::Value> {
        fail!(in self, "Deserializer does not implement deserialize_f64");
    }

    fn deserialize_char<V: Visitor<'de>>(&self, visitor: V, idx: usize) -> Result<V::Value> {
        fail!(in self, "Deserializer does not implement deserialize_char");
    }

    fn deserialize_str<V: Visitor<'de>>(&self, visitor: V, idx: usize) -> Result<V::Value> {
        fail!(in self, "Deserializer does not implement deserialize_str");
    }

    fn deserialize_string<V: Visitor<'de>>(&self, visitor: V, idx: usize) -> Result<V::Value> {
        fail!(in self, "Deserializer does not implement deserialize_string");
    }

    fn deserialize_map<V: Visitor<'de>>(&self, visitor: V, idx: usize) -> Result<V::Value> {
        fail!(in self, "Deserializer does not implement deserialize_map");
    }

    fn deserialize_struct<V: Visitor<'de>>(
        &self,
        name: &'static str,
        fields: &'static [&'static str],
        visitor: V,
        idx: usize,
    ) -> Result<V::Value> {
        fail!(in self, "Deserializer does not implement deserialize_struct");
    }

    fn deserialize_byte_buf<V: Visitor<'de>>(&self, visitor: V, idx: usize) -> Result<V::Value> {
        fail!(in self, "Deserializer does not implement deserialize_byte_buf");
    }

    fn deserialize_bytes<V: Visitor<'de>>(&self, visitor: V, idx: usize) -> Result<V::Value> {
        fail!(in self, "Deserializer does not implement deserialize_bytes");
    }

    fn deserialize_enum<V: Visitor<'de>>(
        &self,
        name: &'static str,
        variants: &'static [&'static str],
        visitor: V,
        idx: usize,
    ) -> Result<V::Value> {
        fail!(in self, "Deserializer does not implement deserialize_enum");
    }

    fn deserialize_identifier<V: Visitor<'de>>(&self, visitor: V, idx: usize) -> Result<V::Value> {
        fail!(in self, "Deserializer does not implement deserialize_identifier");
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
        fail!(in self, "Deserializer does not implement deserialize_tuple");
    }

    fn deserialize_seq<V: Visitor<'de>>(&self, visitor: V, idx: usize) -> Result<V::Value> {
        fail!(in self, "Deserializer does not implement deserialize_seq");
    }

    fn deserialize_tuple_struct<V: Visitor<'de>>(
        &self,
        name: &'static str,
        len: usize,
        visitor: V,
        idx: usize,
    ) -> Result<V::Value> {
        fail!(in self,
            "Deserializer does not implement deserialize_tuple_struct",
        );
    }

    fn deserialize_unit<V: Visitor<'de>>(&self, visitor: V, idx: usize) -> Result<V::Value> {
        fail!(in self, "Deserializer does not implement deserialize_unit");
    }

    fn deserialize_unit_struct<V: Visitor<'de>>(
        &self,
        name: &'static str,
        visitor: V,
        idx: usize,
    ) -> Result<V::Value> {
        fail!(in self,
            "Deserializer does not implement deserialize_unit_struct",
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
