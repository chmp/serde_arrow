use serde::de::{SeqAccess, Visitor};

use crate::internal::{
    error::{fail, Error, Result},
    utils::{Mut, Offset},
};

use super::{simple_deserializer::SimpleDeserializer, utils::BitBuffer};

pub struct BinaryDeserializer<'a, O: Offset> {
    pub buffer: &'a [u8],
    pub offsets: &'a [O],
    pub validity: Option<BitBuffer<'a>>,
    pub next: (usize, usize),
}

impl<'a, O: Offset> BinaryDeserializer<'a, O> {
    pub fn new(buffer: &'a [u8], offsets: &'a [O], validity: Option<BitBuffer<'a>>) -> Self {
        Self {
            buffer,
            offsets,
            validity,
            next: (0, 0),
        }
    }

    pub fn peek_next(&self) -> Result<bool> {
        if self.next.0 + 1 >= self.offsets.len() {
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
        if item + 1 >= self.offsets.len() {
            fail!("called next_slices on exhausted BinaryDeserializer");
        }
        let end = self.offsets[item + 1].try_into_usize()?;
        let start = self.offsets[item].try_into_usize()?;
        self.next = (item + 1, 0);

        Ok(&self.buffer[start..end])
    }
}

impl<'a, O: Offset> SimpleDeserializer<'a> for BinaryDeserializer<'a, O> {
    fn name() -> &'static str {
        "BinaryDeserializer"
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

impl<'de, O: Offset> SeqAccess<'de> for BinaryDeserializer<'de, O> {
    type Error = Error;

    fn next_element_seed<T: serde::de::DeserializeSeed<'de>>(
        &mut self,
        seed: T,
    ) -> Result<Option<T::Value>> {
        let (item, offset) = self.next;
        if item + 1 >= self.offsets.len() {
            return Ok(None);
        }
        let end = self.offsets[item + 1].try_into_usize()?;
        let start = self.offsets[item].try_into_usize()?;

        if offset >= end - start {
            self.next = (item + 1, 0);
            return Ok(None);
        }
        self.next = (item, offset + 1);

        let mut item_deserializer = U8Deserializer(self.buffer[start + offset]);
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
