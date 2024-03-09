use serde::de::Visitor;

use crate::{
    internal::{common::BitBuffer, error::fail, serialization_ng::utils::Mut},
    Result,
};

use super::simple_deserializer::SimpleDeserializer;

pub trait Integer: Sized {
    fn deserialize_any<'de, S: SimpleDeserializer<'de>, V: Visitor<'de>>(
        deser: &mut S,
        visitor: V,
    ) -> Result<V::Value>;

    fn into_bool(&self) -> Result<bool>;

    fn into_i8(&self) -> Result<i8>;
    fn into_i16(&self) -> Result<i16>;
    fn into_i32(&self) -> Result<i32>;
    fn into_i64(&self) -> Result<i64>;

    fn into_u8(&self) -> Result<u8>;
    fn into_u16(&self) -> Result<u16>;
    fn into_u32(&self) -> Result<u32>;
    fn into_u64(&self) -> Result<u64>;
}

pub struct IntegerDeserializer<'a, T: Integer> {
    pub buffer: &'a [T],
    pub validity: Option<BitBuffer<'a>>,
    pub next: usize,
}

impl<'a, T: Integer> IntegerDeserializer<'a, T> {
    pub fn new(buffer: &'a [T], validity: Option<BitBuffer<'a>>) -> Self {
        Self {
            buffer,
            validity,
            next: 0,
        }
    }

    fn next(&mut self) -> Result<Option<&T>> {
        if self.next >= self.buffer.len() {
            fail!("Exhausted PrimitiveDeserializer");
        }
        if let Some(validty) = &self.validity {
            if !validty.is_set(self.next) {
                self.next += 1;
                return Ok(None);
            }
        }

        let val = &self.buffer[self.next];
        self.next += 1;
        Ok(Some(val))
    }

    fn next_required(&mut self) -> Result<&T> {
        if let Some(val) = self.next()? {
            Ok(val)
        } else {
            fail!("Missing value");
        }
    }

    fn peek_next(&self) -> Result<bool> {
        if self.next >= self.buffer.len() {
            fail!("Exhausted PrimitiveDeserializer");
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

impl<'de, T: Integer> SimpleDeserializer<'de> for IntegerDeserializer<'de, T> {
    fn name() -> &'static str {
        "IntegerDeserializer"
    }

    fn deserialize_any<V: serde::de::Visitor<'de>>(&mut self, visitor: V) -> Result<V::Value> {
        if self.peek_next()? {
            T::deserialize_any(self, visitor)
        } else {
            self.consume_next();
            visitor.visit_none()
        }
    }

    fn deserialize_option<V: serde::de::Visitor<'de>>(&mut self, visitor: V) -> Result<V::Value> {
        if self.peek_next()? {
            visitor.visit_some(Mut(self))
        } else {
            self.consume_next();
            visitor.visit_none()
        }
    }

    fn deserialize_bool<V: serde::de::Visitor<'de>>(&mut self, visitor: V) -> Result<V::Value> {
        visitor.visit_bool(self.next_required()?.into_bool()?)
    }

    fn deserialize_char<V: serde::de::Visitor<'de>>(&mut self, visitor: V) -> Result<V::Value> {
        visitor.visit_char(self.next_required()?.into_u32()?.try_into()?)
    }

    fn deserialize_u8<V: serde::de::Visitor<'de>>(&mut self, visitor: V) -> Result<V::Value> {
        visitor.visit_u8(self.next_required()?.into_u8()?)
    }

    fn deserialize_u16<V: serde::de::Visitor<'de>>(&mut self, visitor: V) -> Result<V::Value> {
        visitor.visit_u16(self.next_required()?.into_u16()?)
    }

    fn deserialize_u32<V: serde::de::Visitor<'de>>(&mut self, visitor: V) -> Result<V::Value> {
        visitor.visit_u32(self.next_required()?.into_u32()?)
    }

    fn deserialize_u64<V: serde::de::Visitor<'de>>(&mut self, visitor: V) -> Result<V::Value> {
        visitor.visit_u64(self.next_required()?.into_u64()?)
    }

    fn deserialize_i8<V: serde::de::Visitor<'de>>(&mut self, visitor: V) -> Result<V::Value> {
        visitor.visit_i8(self.next_required()?.into_i8()?)
    }

    fn deserialize_i16<V: serde::de::Visitor<'de>>(&mut self, visitor: V) -> Result<V::Value> {
        visitor.visit_i16(self.next_required()?.into_i16()?)
    }

    fn deserialize_i32<V: serde::de::Visitor<'de>>(&mut self, visitor: V) -> Result<V::Value> {
        visitor.visit_i32(self.next_required()?.into_i32()?)
    }

    fn deserialize_i64<V: serde::de::Visitor<'de>>(&mut self, visitor: V) -> Result<V::Value> {
        visitor.visit_i64(self.next_required()?.into_i64()?)
    }
}