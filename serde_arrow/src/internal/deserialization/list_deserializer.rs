use serde::de::{SeqAccess, Visitor};

use crate::internal::{
    common::{BitBuffer, Mut},
    error::{fail, Error, Result},
};

use super::{array_deserializer::ArrayDeserializer, simple_deserializer::SimpleDeserializer};

pub trait IntoUsize: Copy {
    fn into_usize(&self) -> Result<usize>;
}

impl IntoUsize for i32 {
    fn into_usize(&self) -> Result<usize> {
        Ok((*self).try_into()?)
    }
}

impl IntoUsize for i64 {
    fn into_usize(&self) -> Result<usize> {
        Ok((*self).try_into()?)
    }
}

pub struct ListDeserializer<'a, O: IntoUsize> {
    pub item: Box<ArrayDeserializer<'a>>,
    pub offsets: &'a [O],
    pub validity: Option<BitBuffer<'a>>,
    pub next: (usize, usize),
}

impl<'a, O: IntoUsize> ListDeserializer<'a, O> {
    pub fn new(
        item: ArrayDeserializer<'a>,
        offsets: &'a [O],
        validity: Option<BitBuffer<'a>>,
    ) -> Self {
        Self {
            item: Box::new(item),
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
}

impl<'a, O: IntoUsize> SimpleDeserializer<'a> for ListDeserializer<'a, O> {
    fn name() -> &'static str {
        "ListDeserializer"
    }

    fn deserialize_any<V: Visitor<'a>>(&mut self, visitor: V) -> Result<V::Value> {
        if self.peek_next()? {
            self.deserialize_seq(visitor)
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
}

impl<'de, O: IntoUsize> SeqAccess<'de> for ListDeserializer<'de, O> {
    type Error = Error;

    fn next_element_seed<T: serde::de::DeserializeSeed<'de>>(
        &mut self,
        seed: T,
    ) -> Result<Option<T::Value>> {
        let (item, offset) = self.next;
        if item + 1 >= self.offsets.len() {
            return Ok(None);
        }
        let end = self.offsets[item + 1].into_usize()?;
        let start = self.offsets[item].into_usize()?;

        if offset >= end - start {
            self.next = (item + 1, 0);
            return Ok(None);
        }
        self.next = (item, offset + 1);

        let item = seed.deserialize(Mut(self.item.as_mut()))?;
        Ok(Some(item))
    }
}
