use marrow::view::{BitsWithOffset, FixedSizeListView};
use serde::de::Visitor;

use crate::internal::{
    error::{fail, set_default, Context, Result},
    utils::ChildName,
};

use super::{
    array_deserializer::{get_strategy, ArrayDeserializer},
    list_deserializer::ListItemDeserializer,
    random_access_deserializer::RandomAccessDeserializer,
    utils::bitset_is_set,
};

pub struct FixedSizeListDeserializer<'a> {
    pub path: String,
    pub item: Box<ArrayDeserializer<'a>>,
    pub validity: Option<BitsWithOffset<'a>>,
    pub len: usize,
    pub n: usize,
}

impl<'a> FixedSizeListDeserializer<'a> {
    pub fn new(path: String, view: FixedSizeListView<'a>) -> Result<Self> {
        let child_path = format!("{path}.{child}", child = ChildName(&view.meta.name));
        let item = ArrayDeserializer::new(
            child_path,
            get_strategy(&view.meta)?.as_ref(),
            *view.elements,
        )?;

        Ok(Self {
            path,
            item: Box::new(item),
            validity: view.validity,
            len: view.len,
            n: view.n.try_into()?,
        })
    }
}

impl Context for FixedSizeListDeserializer<'_> {
    fn annotate(&self, annotations: &mut std::collections::BTreeMap<String, String>) {
        set_default(annotations, "field", &self.path);
        set_default(annotations, "data_type", "FixedSizeList(..)");
    }
}

impl<'de> RandomAccessDeserializer<'de> for FixedSizeListDeserializer<'de> {
    fn is_some(&self, idx: usize) -> Result<bool> {
        if idx >= self.len {
            fail!("Out of bounds access");
        }
        if let Some(validity) = self.validity.as_ref() {
            return bitset_is_set(validity, idx);
        }
        Ok(true)
    }

    fn deserialize_any_some<V: Visitor<'de>>(&self, visitor: V, idx: usize) -> Result<V::Value> {
        self.deserialize_seq(visitor, idx)
    }

    fn deserialize_seq<V: Visitor<'de>>(&self, visitor: V, idx: usize) -> Result<V::Value> {
        if idx >= self.len {
            fail!("Out of bounds access");
        }
        visitor.visit_seq(ListItemDeserializer {
            item: self.item.as_ref(),
            start: idx * self.n,
            end: (idx + 1) * self.n,
        })
    }
}
