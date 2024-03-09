use serde::de::{value::StrDeserializer, IgnoredAny, MapAccess, Visitor};

use crate::{
    internal::{common::BitBuffer, error::fail, serialization_ng::utils::Mut},
    Error, Result,
};

use super::{array_deserializer::ArrayDeserializer, simple_deserializer::SimpleDeserializer};

pub struct StructDeserializer<'a> {
    pub fields: Vec<(String, ArrayDeserializer<'a>)>,
    pub validity: Option<BitBuffer<'a>>,
    pub next: (usize, usize),
    pub len: usize,
}

impl<'a> StructDeserializer<'a> {
    pub fn new(
        fields: Vec<(String, ArrayDeserializer<'a>)>,
        validity: Option<BitBuffer<'a>>,
        len: usize,
    ) -> Self {
        Self {
            fields,
            validity,
            len,
            next: (0, 0),
        }
    }

    pub fn peek_next(&self) -> Result<bool> {
        if self.next.0 >= self.len {
            fail!("Exhausted StructDeserializer");
        }
        if let Some(validity) = &self.validity {
            Ok(validity.is_set(self.next.0))
        } else {
            Ok(true)
        }
    }

    pub fn consume_next(&mut self) {
        self.next = (self.next.0 + 1, 0)
    }
}

impl<'de> SimpleDeserializer<'de> for StructDeserializer<'de> {
    fn name() -> &'static str {
        "StructDeserializer"
    }

    fn deserialize_any<V: Visitor<'de>>(&mut self, visitor: V) -> Result<V::Value> {
        if self.peek_next()? {
            visitor.visit_map(self)
        } else {
            self.consume_next();
            for (_, field) in &mut self.fields {
                field.deserialize_ignored_any(IgnoredAny)?;
            }
            visitor.visit_none()
        }
    }

    fn deserialize_option<V: Visitor<'de>>(&mut self, visitor: V) -> Result<V::Value> {
        if self.peek_next()? {
            visitor.visit_some(Mut(self))
        } else {
            self.consume_next();
            for (_, field) in &mut self.fields {
                field.deserialize_ignored_any(IgnoredAny)?;
            }
            visitor.visit_none()
        }
    }

    fn deserialize_map<V: Visitor<'de>>(&mut self, visitor: V) -> Result<V::Value> {
        visitor.visit_map(self)
    }

    fn deserialize_struct<V: Visitor<'de>>(
        &mut self,
        _: &'static str,
        _: &'static [&'static str],
        visitor: V,
    ) -> Result<V::Value> {
        visitor.visit_map(self)
    }
}

impl<'de> MapAccess<'de> for StructDeserializer<'de> {
    type Error = Error;

    fn next_key_seed<K: serde::de::DeserializeSeed<'de>>(
        &mut self,
        seed: K,
    ) -> Result<Option<K::Value>> {
        let (item, field) = self.next;
        if item >= self.len {
            return Ok(None);
        }
        if field >= self.fields.len() {
            self.next = (item + 1, 0);
            return Ok(None);
        }

        let key = seed.deserialize(StrDeserializer::<Error>::new(&self.fields[field].0))?;
        Ok(Some(key))
    }

    fn next_value_seed<V: serde::de::DeserializeSeed<'de>>(&mut self, seed: V) -> Result<V::Value> {
        let (item, field) = self.next;
        self.next = (item, field + 1);

        seed.deserialize(Mut(&mut self.fields[field].1))
    }
}
