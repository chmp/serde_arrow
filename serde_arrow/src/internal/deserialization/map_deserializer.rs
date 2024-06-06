use serde::de::{DeserializeSeed, MapAccess, Visitor};

use crate::internal::{
    deserialization::list_deserializer::IntoUsize,
    error::{fail, Error, Result},
    utils::Mut,
};

use super::{
    array_deserializer::ArrayDeserializer, simple_deserializer::SimpleDeserializer,
    utils::BitBuffer,
};

pub struct MapDeserializer<'a> {
    key: Box<ArrayDeserializer<'a>>,
    value: Box<ArrayDeserializer<'a>>,
    offsets: &'a [i32],
    validity: Option<BitBuffer<'a>>,
    next: (usize, usize),
}

impl<'a> MapDeserializer<'a> {
    pub fn new(
        key: ArrayDeserializer<'a>,
        value: ArrayDeserializer<'a>,
        offsets: &'a [i32],
        validity: Option<BitBuffer<'a>>,
    ) -> Self {
        Self {
            key: Box::new(key),
            value: Box::new(value),
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

impl<'de> SimpleDeserializer<'de> for MapDeserializer<'de> {
    fn name() -> &'static str {
        "MapDeserializer"
    }

    fn deserialize_any<V: Visitor<'de>>(&mut self, visitor: V) -> Result<V::Value> {
        if self.peek_next()? {
            self.deserialize_map(visitor)
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

    fn deserialize_map<V: Visitor<'de>>(&mut self, visitor: V) -> Result<V::Value> {
        visitor.visit_map(self)
    }
}

impl<'de> MapAccess<'de> for MapDeserializer<'de> {
    type Error = Error;

    fn next_key_seed<K: DeserializeSeed<'de>>(
        &mut self,
        seed: K,
    ) -> Result<Option<K::Value>, Self::Error> {
        let (item, entry) = self.next;
        if item + 1 >= self.offsets.len() {
            fail!("Exhausted MapDeserializer");
        }
        let start = self.offsets[item].into_usize()?;
        let end = self.offsets[item + 1].into_usize()?;

        if entry >= (end - start) {
            self.next = (item + 1, 0);
            return Ok(None);
        }
        let res = seed.deserialize(Mut(self.key.as_mut()))?;
        Ok(Some(res))
    }

    fn next_value_seed<V: DeserializeSeed<'de>>(
        &mut self,
        seed: V,
    ) -> Result<V::Value, Self::Error> {
        let (item, entry) = self.next;
        self.next = (item, entry + 1);
        seed.deserialize(Mut(self.value.as_mut()))
    }
}
