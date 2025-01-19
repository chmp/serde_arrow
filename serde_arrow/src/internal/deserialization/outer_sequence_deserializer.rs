use serde::de::{SeqAccess, Visitor};

use crate::internal::{
    error::{Context, Error, Result},
    utils::Mut,
};

use super::{
    array_deserializer::ArrayDeserializer, simple_deserializer::SimpleDeserializer,
    struct_deserializer::StructDeserializer,
};

pub struct OuterSequenceDeserializer<'a> {
    pub item: StructDeserializer<'a>,
    pub next: usize,
    pub len: usize,
}

impl<'a> OuterSequenceDeserializer<'a> {
    pub fn new(fields: Vec<(String, ArrayDeserializer<'a>)>, len: usize) -> Self {
        Self {
            item: StructDeserializer::new(String::from("$"), fields, None, len),
            next: 0,
            len,
        }
    }
}

impl Context for OuterSequenceDeserializer<'_> {
    fn annotate(&self, _: &mut std::collections::BTreeMap<String, String>) {}
}

impl<'de> SimpleDeserializer<'de> for OuterSequenceDeserializer<'de> {
    fn deserialize_newtype_struct<V: Visitor<'de>>(
        &mut self,
        _: &'static str,
        visitor: V,
    ) -> Result<V::Value> {
        visitor.visit_newtype_struct(Mut(self))
    }

    fn deserialize_any<V: Visitor<'de>>(&mut self, visitor: V) -> Result<V::Value> {
        self.deserialize_seq(visitor)
    }

    fn deserialize_seq<V: Visitor<'de>>(&mut self, visitor: V) -> Result<V::Value> {
        visitor.visit_seq(self)
    }

    fn deserialize_tuple<V: Visitor<'de>>(&mut self, _: usize, visitor: V) -> Result<V::Value> {
        visitor.visit_seq(self)
    }

    fn deserialize_tuple_struct<V: Visitor<'de>>(
        &mut self,
        _: &'static str,
        _: usize,
        visitor: V,
    ) -> Result<V::Value> {
        visitor.visit_seq(self)
    }
}

impl<'de> SeqAccess<'de> for OuterSequenceDeserializer<'de> {
    type Error = Error;

    fn next_element_seed<T: serde::de::DeserializeSeed<'de>>(
        &mut self,
        seed: T,
    ) -> Result<Option<T::Value>, Self::Error> {
        if self.next >= self.len {
            return Ok(None);
        }
        self.next += 1;

        let item = seed.deserialize(Mut(&mut self.item))?;
        Ok(Some(item))
    }
}
