use marrow::view::BitsWithOffset;
use serde::de::{DeserializeSeed, MapAccess, Visitor};

use crate::internal::{
    error::{fail, set_default, try_, Context, ContextSupport, Error, Result},
    utils::Offset,
};

use super::{
    array_deserializer::ArrayDeserializer,
    random_access_deserializer::RandomAccessDeserializer,
    simple_deserializer::SimpleDeserializer,
    utils::bitset_is_set,
};

pub struct MapDeserializer<'a> {
    path: String,
    key: Box<ArrayDeserializer<'a>>,
    value: Box<ArrayDeserializer<'a>>,
    offsets: &'a [i32],
    validity: Option<BitsWithOffset<'a>>,
}

impl<'a> MapDeserializer<'a> {
    pub fn new(
        path: String,
        key: ArrayDeserializer<'a>,
        value: ArrayDeserializer<'a>,
        offsets: &'a [i32],
        validity: Option<BitsWithOffset<'a>>,
    ) -> Result<Self> {
        Ok(Self {
            path,
            key: Box::new(key),
            value: Box::new(value),
            offsets,
            validity,
        })
    }
}

impl Context for MapDeserializer<'_> {
    fn annotate(&self, annotations: &mut std::collections::BTreeMap<String, String>) {
        set_default(annotations, "field", &self.path);
        set_default(annotations, "data_type", "Map(..)");
    }
}

impl<'de> SimpleDeserializer<'de> for MapDeserializer<'de> {}

impl<'de> RandomAccessDeserializer<'de> for MapDeserializer<'de> {
    fn is_some(&self, idx: usize) -> Result<bool> {
        if idx + 1 >= self.offsets.len() {
            fail!("Out of bounds access")
        }
        if let Some(validity) = &self.validity {
            Ok(bitset_is_set(validity, idx)?)
        } else {
            Ok(true)
        }
    }

    fn deserialize_any_some<V: Visitor<'de>>(&self, visitor: V, idx: usize) -> Result<V::Value> {
        self.deserialize_map(visitor, idx)
    }

    fn deserialize_map<V: Visitor<'de>>(&self, visitor: V, idx: usize) -> Result<V::Value> {
        try_(|| {
            if idx + 1 >= self.offsets.len() {
                fail!("Out of bounds access")
            }

            visitor.visit_map(MapItemDeserializer {
                deserializer: self,
                start: self.offsets[idx].try_into_usize()?,
                end: self.offsets[idx + 1].try_into_usize()?,
            })
        })
        .ctx(self)
    }
}

struct MapItemDeserializer<'this, 'de> {
    deserializer: &'this MapDeserializer<'de>,
    start: usize,
    end: usize,
}

impl<'de> MapAccess<'de> for MapItemDeserializer<'_, 'de> {
    type Error = Error;

    fn next_key_seed<K: DeserializeSeed<'de>>(&mut self, seed: K) -> Result<Option<K::Value>> {
        if self.start >= self.end {
            return Ok(None);
        }
        let key = seed.deserialize(self.deserializer.key.at(self.start))?;
        Ok(Some(key))
    }

    fn next_value_seed<V: DeserializeSeed<'de>>(&mut self, seed: V) -> Result<V::Value> {
        if self.start >= self.end {
            fail!("Invalid state in MapItemDeserializer");
        }
        let value = seed.deserialize(self.deserializer.value.at(self.start))?;
        self.start += 1;
        Ok(value)
    }
}
