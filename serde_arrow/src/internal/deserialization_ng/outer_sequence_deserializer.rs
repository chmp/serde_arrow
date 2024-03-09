use serde::de::SeqAccess;

use crate::{internal::serialization_ng::utils::Mut, Error, Result};

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
            item: StructDeserializer::new(fields, None, len),
            next: 0,
            len,
        }
    }
}

impl<'de> SimpleDeserializer<'de> for OuterSequenceDeserializer<'de> {
    fn name() -> &'static str {
        "OuterSequenceDeserializer"
    }

    fn deserialize_any<V: serde::de::Visitor<'de>>(&mut self, visitor: V) -> Result<V::Value> {
        self.deserialize_seq(visitor)
    }

    fn deserialize_seq<V: serde::de::Visitor<'de>>(&mut self, visitor: V) -> Result<V::Value> {
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
