use serde::de::{SeqAccess, Visitor};

use crate::internal::{
    error::{fail, Error, Result},
    utils::Mut,
};

use super::{simple_deserializer::SimpleDeserializer, utils::BitBuffer};

pub struct FixedSizeBinaryDeserializer<'a> {
    pub buffer: &'a [u8],
    pub validity: Option<BitBuffer<'a>>,
    pub next: (usize, usize),
    pub shape: (usize, usize),
}

impl<'a> FixedSizeBinaryDeserializer<'a> {
    pub fn new(shape: (usize, usize), buffer: &'a [u8], validity: Option<BitBuffer<'a>>) -> Self {
        Self {
            buffer,
            validity,
            shape,
            next: (0, 0),
        }
    }

    pub fn peek_next(&self) -> Result<bool> {
        if self.next.0 >= self.shape.0 {
            fail!("Exhausted ListDeserializer")
        }
        if let Some(validity) = &self.validity {
            Ok(validity.is_set(self.next.0))
        } else {
            Ok(true)
        }
    }

    pub fn consume_next(&mut self) {
        self.next = (self.next.0 + 1, 0);
    }

    pub fn next_slice(&mut self) -> Result<&'a [u8]> {
        let (item, _) = self.next;
        if item >= self.shape.0 {
            fail!("called next_slices on exhausted BinaryDeserializer");
        }
        self.next = (item + 1, 0);

        Ok(&self.buffer[item * self.shape.1..(item + 1) * self.shape.1])
    }
}

impl<'a> SimpleDeserializer<'a> for FixedSizeBinaryDeserializer<'a> {
    fn name() -> &'static str {
        "FixedSizeBinaryDeserializer"
    }

    fn deserialize_any<V: Visitor<'a>>(&mut self, visitor: V) -> Result<V::Value> {
        if self.peek_next()? {
            self.deserialize_bytes(visitor)
        } else {
            self.consume_next();
            visitor.visit_none()
        }
    }

    fn deserialize_option<V: Visitor<'a>>(&mut self, visitor: V) -> Result<V::Value> {
        if self.peek_next()? {
            visitor.visit_some(Mut(self))
        } else {
            self.consume_next();
            visitor.visit_none()
        }
    }

    fn deserialize_seq<V: Visitor<'a>>(&mut self, visitor: V) -> Result<V::Value> {
        visitor.visit_seq(self)
    }

    fn deserialize_bytes<V: Visitor<'a>>(&mut self, visitor: V) -> Result<V::Value> {
        visitor.visit_borrowed_bytes(self.next_slice()?)
    }

    fn deserialize_byte_buf<V: Visitor<'a>>(&mut self, visitor: V) -> Result<V::Value> {
        visitor.visit_borrowed_bytes(self.next_slice()?)
    }
}

impl<'de> SeqAccess<'de> for FixedSizeBinaryDeserializer<'de> {
    type Error = Error;

    fn next_element_seed<T: serde::de::DeserializeSeed<'de>>(
        &mut self,
        seed: T,
    ) -> Result<Option<T::Value>> {
        let (item, offset) = self.next;
        if item >= self.shape.0 {
            return Ok(None);
        }
        if offset >= self.shape.1 {
            self.next = (item + 1, 0);
            return Ok(None);
        }
        self.next = (item, offset + 1);
        let mut item_deserializer = U8Deserializer(self.buffer[item * self.shape.1 + offset]);
        let item = seed.deserialize(Mut(&mut item_deserializer))?;
        Ok(Some(item))
    }
}

struct U8Deserializer(u8);

impl<'de> SimpleDeserializer<'de> for U8Deserializer {
    fn name() -> &'static str {
        "U8Deserializer"
    }

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
