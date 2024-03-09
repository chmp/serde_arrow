use serde::de::{value::StrDeserializer, MapAccess};

use crate::{internal::serialization_ng::utils::Mut, Error, Result};

use super::{array_deserializer::ArrayDeserializer, simple_deserializer::SimpleDeserializer};


pub struct StructDeserializer<'a> {
    pub fields: Vec<(String, ArrayDeserializer<'a>)>,
    pub next: (usize, usize),
    pub len: usize,
}

impl<'a> StructDeserializer<'a> {
    pub fn new(fields: Vec<(String, ArrayDeserializer<'a>)>, len: usize) -> Self {
        Self {
            fields,
            len,
            next: (0, 0),
        }
    }
}

impl<'de> SimpleDeserializer<'de> for StructDeserializer<'de> {
    fn name() -> &'static str {
        "StructDeserializer"
    }

    fn deserialize_any<V: serde::de::Visitor<'de>>(&mut self, visitor: V) -> Result<V::Value> {
        visitor.visit_map(self)
    }

    fn deserialize_map<V: serde::de::Visitor<'de>>(&mut self, visitor: V) -> Result<V::Value> {
        visitor.visit_map(self)
    }

    fn deserialize_struct<V: serde::de::Visitor<'de>>(
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

    fn next_key_seed<K: serde::de::DeserializeSeed<'de>>(&mut self, seed: K) -> Result<Option<K::Value>> {
        let (item, field) = self.next;
        if item >= self.len  {
            return Ok(None)
        }
        if field >= self.fields.len() {
            self.next = (item + 1, 0);
            return Ok(None)
        }

        let key = seed.deserialize(StrDeserializer::<Error>::new(&self.fields[field].0))?;
        Ok(Some(key))
    }

    fn next_value_seed<V: serde::de::DeserializeSeed<'de>>(&mut self, seed: V) -> Result<V::Value> {
        let (item, field) = self.next;
        self.next =  (item, field + 1);
        
        seed.deserialize(Mut(&mut self.fields[field].1))
    }
}
