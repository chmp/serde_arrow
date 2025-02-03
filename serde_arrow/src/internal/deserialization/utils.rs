use marrow::view::BitsWithOffset;
use serde::de::{SeqAccess, Visitor};

use crate::internal::{
    error::{Context, Error, Result},
    utils::{array_ext::get_bit_buffer, Mut},
};

use super::simple_deserializer::SimpleDeserializer;

pub fn bitset_is_set(set: &BitsWithOffset<'_>, idx: usize) -> Result<bool> {
    get_bit_buffer(set.data, set.offset, idx)
}

pub struct U8Deserializer(pub u8);

impl Context for U8Deserializer {
    fn annotate(&self, _: &mut std::collections::BTreeMap<String, String>) {}
}

impl<'de> SimpleDeserializer<'de> for U8Deserializer {
    fn deserialize_u8<V: Visitor<'de>>(&mut self, visitor: V) -> Result<V::Value> {
        visitor.visit_u8(self.0)
    }

    fn deserialize_u16<V: Visitor<'de>>(&mut self, visitor: V) -> Result<V::Value> {
        visitor.visit_u16(self.0.into())
    }

    fn deserialize_u32<V: Visitor<'de>>(&mut self, visitor: V) -> Result<V::Value> {
        visitor.visit_u32(self.0.into())
    }

    fn deserialize_u64<V: Visitor<'de>>(&mut self, visitor: V) -> Result<V::Value> {
        visitor.visit_u64(self.0.into())
    }

    fn deserialize_i8<V: Visitor<'de>>(&mut self, visitor: V) -> Result<V::Value> {
        visitor.visit_i8(self.0.try_into()?)
    }

    fn deserialize_i16<V: Visitor<'de>>(&mut self, visitor: V) -> Result<V::Value> {
        visitor.visit_i16(self.0.into())
    }

    fn deserialize_i32<V: Visitor<'de>>(&mut self, visitor: V) -> Result<V::Value> {
        visitor.visit_i32(self.0.into())
    }

    fn deserialize_i64<V: Visitor<'de>>(&mut self, visitor: V) -> Result<V::Value> {
        visitor.visit_i64(self.0.into())
    }
}

pub struct U8SliceDeserializer<'a>(&'a [u8], usize);

impl<'a> U8SliceDeserializer<'a> {
    pub fn new(bytes: &'a [u8]) -> Self {
        Self(bytes, 0)
    }
}

impl<'de> SeqAccess<'de> for U8SliceDeserializer<'de> {
    type Error = Error;

    fn size_hint(&self) -> Option<usize> {
        Some(self.0.len())
    }

    fn next_element_seed<T: serde::de::DeserializeSeed<'de>>(
        &mut self,
        seed: T,
    ) -> Result<Option<T::Value>> {
        let U8SliceDeserializer(bytes, idx) = *self;
        if idx >= bytes.len() {
            return Ok(None);
        }

        let mut item_deserializer = U8Deserializer(bytes[idx]);
        let item = seed.deserialize(Mut(&mut item_deserializer))?;

        self.1 = idx + 1;

        Ok(Some(item))
    }
}
