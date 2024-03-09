use crate::{
    internal::{common::BitBuffer, error::fail, serialization_ng::utils::Mut},
    Result,
};

use super::simple_deserializer::SimpleDeserializer;

pub struct BoolDeserializer<'a> {
    pub buffer: BitBuffer<'a>,
    pub validity: Option<BitBuffer<'a>>,
    pub next: usize,
}

impl<'a> BoolDeserializer<'a> {
    pub fn new(buffer: BitBuffer<'a>, validity: Option<BitBuffer<'a>>) -> Self {
        Self {
            buffer,
            validity,
            next: 0,
        }
    }

    fn next(&mut self) -> Result<Option<bool>> {
        if self.next >= self.buffer.len() {
            fail!("Exhausted PrimitiveDeserializer");
        }
        if let Some(validty) = &self.validity {
            if !validty.is_set(self.next) {
                self.next += 1;
                return Ok(None);
            }
        }

        let val = self.buffer.is_set(self.next);
        self.next += 1;
        Ok(Some(val))
    }

    fn next_required(&mut self) -> Result<bool> {
        if let Some(val) = self.next()? {
            Ok(val)
        } else {
            fail!("Missing value");
        }
    }

    fn next_is_present(&self) -> bool {
        if self.next >= self.buffer.len() {
            false
        } else if let Some(validity) = &self.validity {
            validity.is_set(self.next)
        } else {
            true
        }
    }

    fn consume_next(&mut self) {
        self.next += 1;
    }
}

impl<'de> SimpleDeserializer<'de> for BoolDeserializer<'de> {
    fn name() -> &'static str {
        "BoolDeserializer"
    }

    fn deserialize_any<V: serde::de::Visitor<'de>>(&mut self, visitor: V) -> Result<V::Value> {
        if self.next_is_present() {
            self.deserialize_bool(visitor)
        } else {
            self.consume_next();
            visitor.visit_none()
        }
    }

    fn deserialize_option<V: serde::de::Visitor<'de>>(&mut self, visitor: V) -> Result<V::Value> {
        if self.next_is_present() {
            visitor.visit_some(Mut(self))
        } else {
            self.consume_next();
            visitor.visit_none()
        }
    }

    fn deserialize_bool<V: serde::de::Visitor<'de>>(&mut self, visitor: V) -> Result<V::Value> {
        visitor.visit_bool(self.next_required()?)
    }

    fn deserialize_u8<V: serde::de::Visitor<'de>>(&mut self, visitor: V) -> Result<V::Value> {
        visitor.visit_u8(if self.next_required()? { 1 } else { 0 })
    }

    fn deserialize_u16<V: serde::de::Visitor<'de>>(&mut self, visitor: V) -> Result<V::Value> {
        visitor.visit_u16(if self.next_required()? { 1 } else { 0 })
    }

    fn deserialize_u32<V: serde::de::Visitor<'de>>(&mut self, visitor: V) -> Result<V::Value> {
        visitor.visit_u32(if self.next_required()? { 1 } else { 0 })
    }

    fn deserialize_u64<V: serde::de::Visitor<'de>>(&mut self, visitor: V) -> Result<V::Value> {
        visitor.visit_u64(if self.next_required()? { 1 } else { 0 })
    }

    fn deserialize_i8<V: serde::de::Visitor<'de>>(&mut self, visitor: V) -> Result<V::Value> {
        visitor.visit_i8(if self.next_required()? { 1 } else { 0 })
    }

    fn deserialize_i16<V: serde::de::Visitor<'de>>(&mut self, visitor: V) -> Result<V::Value> {
        visitor.visit_i16(if self.next_required()? { 1 } else { 0 })
    }

    fn deserialize_i32<V: serde::de::Visitor<'de>>(&mut self, visitor: V) -> Result<V::Value> {
        visitor.visit_i32(if self.next_required()? { 1 } else { 0 })
    }

    fn deserialize_i64<V: serde::de::Visitor<'de>>(&mut self, visitor: V) -> Result<V::Value> {
        visitor.visit_i64(if self.next_required()? { 1 } else { 0 })
    }
}
