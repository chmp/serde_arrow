use serde::de::Visitor;

use crate::internal::{error::fail, error::Result, utils::Mut};

use super::{simple_deserializer::SimpleDeserializer, utils::BitBuffer};

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
            fail!("Exhausted BoolDeserializer");
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

    fn peek_next(&self) -> Result<bool> {
        if self.next >= self.buffer.len() {
            fail!("Exhausted BoolDeserializer");
        } else if let Some(validity) = &self.validity {
            Ok(validity.is_set(self.next))
        } else {
            Ok(true)
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

    fn deserialize_any<V: Visitor<'de>>(&mut self, visitor: V) -> Result<V::Value> {
        if self.peek_next()? {
            self.deserialize_bool(visitor)
        } else {
            self.consume_next();
            visitor.visit_none()
        }
    }

    fn deserialize_option<V: Visitor<'de>>(&mut self, visitor: V) -> Result<V::Value> {
        if self.peek_next()? {
            visitor.visit_some(Mut(self))
        } else {
            self.consume_next();
            visitor.visit_none()
        }
    }

    fn deserialize_bool<V: Visitor<'de>>(&mut self, visitor: V) -> Result<V::Value> {
        visitor.visit_bool(self.next_required()?)
    }

    fn deserialize_u8<V: Visitor<'de>>(&mut self, visitor: V) -> Result<V::Value> {
        visitor.visit_u8(if self.next_required()? { 1 } else { 0 })
    }

    fn deserialize_u16<V: Visitor<'de>>(&mut self, visitor: V) -> Result<V::Value> {
        visitor.visit_u16(if self.next_required()? { 1 } else { 0 })
    }

    fn deserialize_u32<V: Visitor<'de>>(&mut self, visitor: V) -> Result<V::Value> {
        visitor.visit_u32(if self.next_required()? { 1 } else { 0 })
    }

    fn deserialize_u64<V: Visitor<'de>>(&mut self, visitor: V) -> Result<V::Value> {
        visitor.visit_u64(if self.next_required()? { 1 } else { 0 })
    }

    fn deserialize_i8<V: Visitor<'de>>(&mut self, visitor: V) -> Result<V::Value> {
        visitor.visit_i8(if self.next_required()? { 1 } else { 0 })
    }

    fn deserialize_i16<V: Visitor<'de>>(&mut self, visitor: V) -> Result<V::Value> {
        visitor.visit_i16(if self.next_required()? { 1 } else { 0 })
    }

    fn deserialize_i32<V: Visitor<'de>>(&mut self, visitor: V) -> Result<V::Value> {
        visitor.visit_i32(if self.next_required()? { 1 } else { 0 })
    }

    fn deserialize_i64<V: Visitor<'de>>(&mut self, visitor: V) -> Result<V::Value> {
        visitor.visit_i64(if self.next_required()? { 1 } else { 0 })
    }
}
