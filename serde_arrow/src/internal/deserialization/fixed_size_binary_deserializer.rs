use marrow::view::FixedSizeBinaryView;
use serde::de::{SeqAccess, Visitor};

use crate::internal::{
    error::{fail, set_default, try_, Context, ContextSupport, Error, Result},
    utils::Mut,
};

use super::{simple_deserializer::SimpleDeserializer, utils::bitset_is_set};

pub struct FixedSizeBinaryDeserializer<'a> {
    pub path: String,
    pub view: FixedSizeBinaryView<'a>,
    pub next: (usize, usize),
    pub shape: (usize, usize),
}

impl<'a> FixedSizeBinaryDeserializer<'a> {
    pub fn new(path: String, view: FixedSizeBinaryView<'a>) -> Result<Self> {
        let n = usize::try_from(view.n)?;
        if view.data.len() % n != 0 {
            fail!(
                concat!(
                    "Invalid FixedSizeBinary array: Data of len {len} is not ",
                    "evenly divisible into chunks of size {n}",
                ),
                len = view.data.len(),
                n = n,
            );
        }

        let shape = (view.data.len() / n, n);
        Ok(Self {
            path,
            view,
            shape,
            next: (0, 0),
        })
    }

    pub fn peek_next(&self) -> Result<bool> {
        if self.next.0 >= self.shape.0 {
            fail!("Exhausted deserializer")
        }
        if let Some(validity) = &self.view.validity {
            Ok(bitset_is_set(validity, self.next.0)?)
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
            fail!("Exhausted deserializer");
        }
        self.next = (item + 1, 0);

        Ok(&self.view.data[item * self.shape.1..(item + 1) * self.shape.1])
    }
}

impl Context for FixedSizeBinaryDeserializer<'_> {
    fn annotate(&self, annotations: &mut std::collections::BTreeMap<String, String>) {
        set_default(annotations, "field", &self.path);
        set_default(annotations, "data_type", "FixedSizeBinary(..)");
    }
}

impl<'a> SimpleDeserializer<'a> for FixedSizeBinaryDeserializer<'a> {
    fn deserialize_any<V: Visitor<'a>>(&mut self, visitor: V) -> Result<V::Value> {
        try_(|| {
            if self.peek_next()? {
                self.deserialize_bytes(visitor)
            } else {
                self.consume_next();
                visitor.visit_none()
            }
        })
        .ctx(self)
    }

    fn deserialize_option<V: Visitor<'a>>(&mut self, visitor: V) -> Result<V::Value> {
        try_(|| {
            if self.peek_next()? {
                visitor.visit_some(Mut(self))
            } else {
                self.consume_next();
                visitor.visit_none()
            }
        })
        .ctx(self)
    }

    fn deserialize_seq<V: Visitor<'a>>(&mut self, visitor: V) -> Result<V::Value> {
        try_(|| visitor.visit_seq(&mut *self)).ctx(self)
    }

    fn deserialize_bytes<V: Visitor<'a>>(&mut self, visitor: V) -> Result<V::Value> {
        try_(|| visitor.visit_borrowed_bytes(self.next_slice()?)).ctx(self)
    }

    fn deserialize_byte_buf<V: Visitor<'a>>(&mut self, visitor: V) -> Result<V::Value> {
        try_(|| visitor.visit_borrowed_bytes(self.next_slice()?)).ctx(self)
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
        let mut item_deserializer = U8Deserializer(self.view.data[item * self.shape.1 + offset]);
        let item = seed.deserialize(Mut(&mut item_deserializer))?;
        Ok(Some(item))
    }
}

struct U8Deserializer(u8);

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
