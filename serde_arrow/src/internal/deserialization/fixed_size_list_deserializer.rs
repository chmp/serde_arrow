use serde::de::{IgnoredAny, SeqAccess, Visitor};

use crate::internal::{
    arrow::BitsWithOffset,
    error::{fail, set_default, try_, Context, ContextSupport, Error, Result},
    utils::Mut,
};

use super::{
    array_deserializer::ArrayDeserializer, simple_deserializer::SimpleDeserializer,
    utils::bitset_is_set,
};

pub struct FixedSizeListDeserializer<'a> {
    pub path: String,
    pub item: Box<ArrayDeserializer<'a>>,
    pub validity: Option<BitsWithOffset<'a>>,
    pub shape: (usize, usize),
    pub next: (usize, usize),
}

impl<'a> FixedSizeListDeserializer<'a> {
    pub fn new(
        path: String,
        item: ArrayDeserializer<'a>,
        validity: Option<BitsWithOffset<'a>>,
        n: usize,
        len: usize,
    ) -> Self {
        Self {
            path,
            item: Box::new(item),
            validity,
            shape: (len, n),
            next: (0, 0),
        }
    }

    pub fn peek_next(&self) -> Result<bool> {
        if self.next.0 >= self.shape.0 {
            fail!("Exhausted deserializer")
        }
        if let Some(validity) = &self.validity {
            Ok(bitset_is_set(validity, self.next.0)?)
        } else {
            Ok(true)
        }
    }

    pub fn consume_next(&mut self) -> Result<()> {
        for _ in 0..self.shape.1 {
            self.item.deserialize_ignored_any(IgnoredAny)?;
        }

        self.next = (self.next.0 + 1, 0);
        Ok(())
    }
}

impl<'a> Context for FixedSizeListDeserializer<'a> {
    fn annotate(&self, annotations: &mut std::collections::BTreeMap<String, String>) {
        set_default(annotations, "field", &self.path);
        set_default(annotations, "data_type", "FixedSizeList(..)");
    }
}

impl<'a> SimpleDeserializer<'a> for FixedSizeListDeserializer<'a> {
    fn deserialize_any<V: Visitor<'a>>(&mut self, visitor: V) -> Result<V::Value> {
        try_(|| {
            if self.peek_next()? {
                self.deserialize_seq(visitor)
            } else {
                self.consume_next()?;
                visitor.visit_none()
            }
        })
        .ctx(self)
    }

    fn deserialize_option<V: Visitor<'a>>(&mut self, visitor: V) -> Result<V::Value> {
        try_(|| {
            if self.peek_next()? {
                visitor.visit_some(Mut(&mut *self))
            } else {
                self.consume_next()?;
                visitor.visit_none()
            }
        })
        .ctx(self)
    }

    fn deserialize_seq<V: Visitor<'a>>(&mut self, visitor: V) -> Result<V::Value> {
        try_(|| visitor.visit_seq(&mut *self)).ctx(self)
    }
}

impl<'de> SeqAccess<'de> for FixedSizeListDeserializer<'de> {
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

        let item = seed.deserialize(Mut(self.item.as_mut()))?;
        Ok(Some(item))
    }
}
