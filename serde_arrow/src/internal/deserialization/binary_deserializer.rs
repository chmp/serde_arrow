use serde::de::{SeqAccess, Visitor};

use crate::internal::{
    arrow::BytesArrayView,
    error::{fail, Context, Error, Result},
    utils::{btree_map, Mut, NamedType, Offset},
};

use super::{simple_deserializer::SimpleDeserializer, utils::bitset_is_set};

pub struct BinaryDeserializer<'a, O: Offset> {
    pub path: String,
    pub view: BytesArrayView<'a, O>,
    pub next: (usize, usize),
}

impl<'a, O: Offset> BinaryDeserializer<'a, O> {
    pub fn new(path: String, view: BytesArrayView<'a, O>) -> Self {
        Self {
            path,
            view,
            next: (0, 0),
        }
    }

    pub fn peek_next(&self) -> Result<bool> {
        if self.next.0 + 1 >= self.view.offsets.len() {
            fail!("Exhausted ListDeserializer")
        }
        if let Some(validity) = &self.view.validity {
            bitset_is_set(validity, self.next.0)
        } else {
            Ok(true)
        }
    }

    pub fn consume_next(&mut self) {
        self.next = (self.next.0 + 1, 0);
    }

    pub fn peek_next_slice_range(&self) -> Result<(usize, usize)> {
        let (item, _) = self.next;
        if item + 1 >= self.view.offsets.len() {
            fail!("called next_slices on exhausted BinaryDeserializer");
        }
        let end = self.view.offsets[item + 1].try_into_usize()?;
        let start = self.view.offsets[item].try_into_usize()?;
        Ok((start, end))
    }

    pub fn next_slice(&mut self) -> Result<&'a [u8]> {
        let (start, end) = self.peek_next_slice_range()?;
        let (item, _) = self.next;
        self.next = (item + 1, 0);
        Ok(&self.view.data[start..end])
    }
}

impl<'a, O: Offset + NamedType> Context for BinaryDeserializer<'a, O> {
    fn annotations(&self) -> std::collections::BTreeMap<String, String> {
        let data_type = match O::NAME {
            "i32" => "Binary",
            "i64" => "LargeBinary",
            _ => "<unknown>",
        };
        btree_map!("path" => self.path.clone(), "data_type" => data_type)
    }
}

impl<'a, O: Offset + NamedType> SimpleDeserializer<'a> for BinaryDeserializer<'a, O> {
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
        let (start, end) = self.peek_next_slice_range()?;

        if offset >= end - start {
            self.next = (item + 1, 0);
            return Ok(None);
        }
        self.next = (item, offset + 1);

        let mut item_deserializer = U8Deserializer(self.view.data[start + offset]);
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
