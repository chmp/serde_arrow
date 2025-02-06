use marrow::view::{BitsWithOffset, MapView};
use serde::de::{DeserializeSeed, MapAccess, Visitor};

use crate::internal::{
    error::{fail, set_default, try_, Context, ContextSupport, Error, Result},
    schema::get_strategy_from_metadata,
    utils::{ChildName, Offset},
};

use super::{
    array_deserializer::ArrayDeserializer, random_access_deserializer::RandomAccessDeserializer,
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
    pub fn new(path: String, view: MapView<'a>) -> Result<Self> {
        let keys_path = format!(
            "{path}.{entries}.{keys}",
            entries = ChildName(&view.meta.entries_name),
            keys = ChildName(&view.meta.keys.name),
        );
        let keys = ArrayDeserializer::new(
            keys_path,
            get_strategy_from_metadata(&view.meta.keys.metadata)?.as_ref(),
            *view.keys,
        )?;

        let values_path = format!(
            "{path}.{entries}.{values}",
            entries = ChildName(&view.meta.entries_name),
            values = ChildName(&view.meta.values.name),
        );
        let values = ArrayDeserializer::new(
            values_path,
            get_strategy_from_metadata(&view.meta.values.metadata)?.as_ref(),
            *view.values,
        )?;

        Ok(Self {
            path,
            key: Box::new(keys),
            value: Box::new(values),
            offsets: view.offsets,
            validity: view.validity,
        })
    }
}

impl Context for MapDeserializer<'_> {
    fn annotate(&self, annotations: &mut std::collections::BTreeMap<String, String>) {
        set_default(annotations, "field", &self.path);
        set_default(annotations, "data_type", "Map(..)");
    }
}

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
