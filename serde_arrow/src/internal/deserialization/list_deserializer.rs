use marrow::view::{BitsWithOffset, ListView};
use serde::de::{DeserializeSeed, SeqAccess, Visitor};

use crate::internal::{
    error::{fail, set_default, try_, Context, ContextSupport, Error, Result},
    schema::get_strategy_from_metadata,
    utils::{ChildName, NamedType, Offset},
};

use super::{
    array_deserializer::ArrayDeserializer, random_access_deserializer::RandomAccessDeserializer,
    utils::bitset_is_set,
};

pub struct ListDeserializer<'a, O: Offset> {
    pub path: String,
    pub item: Box<ArrayDeserializer<'a>>,
    pub offsets: &'a [O],
    pub validity: Option<BitsWithOffset<'a>>,
}

impl<'de, O: Offset> ListDeserializer<'de, O> {
    pub fn new(path: String, view: ListView<'de, O>) -> Result<Self> {
        let child_path = format!("{path}.{child}", child = ChildName(&view.meta.name));
        let item = ArrayDeserializer::new(
            child_path,
            get_strategy_from_metadata(&view.meta.metadata)?.as_ref(),
            *view.elements,
        )?;

        Ok(Self {
            path,
            item: Box::new(item),
            offsets: view.offsets,
            validity: view.validity,
        })
    }

    fn get<'this>(&'this self, idx: usize) -> Result<ListItemDeserializer<'this, 'de>> {
        if idx + 1 >= self.offsets.len() {
            fail!("Outs of bound access");
        }
        Ok(ListItemDeserializer {
            item: self.item.as_ref(),
            start: self.offsets[idx].try_into_usize()?,
            end: self.offsets[idx + 1].try_into_usize()?,
        })
    }
}

impl<O: NamedType + Offset> Context for ListDeserializer<'_, O> {
    fn annotate(&self, annotations: &mut std::collections::BTreeMap<String, String>) {
        set_default(annotations, "filed", &self.path);
        set_default(
            annotations,
            "data_type",
            match O::NAME {
                "i32" => "List(..)",
                "i64" => "LargeList(..)",
                _ => "<unknown>",
            },
        );
    }
}

impl<'de, O: Offset + NamedType> RandomAccessDeserializer<'de> for ListDeserializer<'de, O> {
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
        self.deserialize_seq(visitor, idx)
    }

    fn deserialize_seq<V: Visitor<'de>>(&self, visitor: V, idx: usize) -> Result<V::Value> {
        try_(|| visitor.visit_seq(self.get(idx)?)).ctx(self)
    }

    fn deserialize_bytes<V: Visitor<'de>>(&self, visitor: V, idx: usize) -> Result<V::Value> {
        try_(|| visitor.visit_seq(self.get(idx)?)).ctx(self)
    }

    fn deserialize_byte_buf<V: Visitor<'de>>(&self, visitor: V, idx: usize) -> Result<V::Value> {
        try_(|| visitor.visit_seq(self.get(idx)?)).ctx(self)
    }
}

pub struct ListItemDeserializer<'a, 'de> {
    pub item: &'a ArrayDeserializer<'de>,
    pub start: usize,
    pub end: usize,
}

impl<'de> SeqAccess<'de> for ListItemDeserializer<'_, 'de> {
    type Error = Error;

    fn next_element_seed<T: DeserializeSeed<'de>>(&mut self, seed: T) -> Result<Option<T::Value>> {
        if self.start >= self.end {
            return Ok(None);
        }
        let item = seed.deserialize(self.item.at(self.start))?;
        self.start += 1;
        Ok(Some(item))
    }
}
