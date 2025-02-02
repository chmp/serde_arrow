use marrow::view::BooleanView;
use serde::de::Visitor;

use crate::internal::error::{fail, set_default, try_, Context, ContextSupport, Error, Result};

use super::{
    random_access_deserializer::RandomAccessDeserializer, simple_deserializer::SimpleDeserializer,
    utils::bitset_is_set,
};

pub struct BoolDeserializer<'a> {
    pub path: String,
    pub view: BooleanView<'a>,
}

impl<'a> BoolDeserializer<'a> {
    pub fn new(path: String, view: BooleanView<'a>) -> Self {
        Self { path, view }
    }

    fn get(&self, idx: usize) -> Result<Option<bool>> {
        if idx >= self.view.len {
            fail!("Out of bounds access");
        }
        if let Some(validity) = &self.view.validity {
            if !bitset_is_set(validity, idx)? {
                return Ok(None);
            }
        }

        Ok(Some(bitset_is_set(&self.view.values, idx)?))
    }

    fn get_required(&self, idx: usize) -> Result<bool> {
        if let Some(value) = self.get(idx)? {
            Ok(value)
        } else {
            fail!("Required value was not defined");
        }
    }
}

impl Context for BoolDeserializer<'_> {
    fn annotate(&self, annotations: &mut std::collections::BTreeMap<String, String>) {
        set_default(annotations, "field", &self.path);
        set_default(annotations, "data_type", "Boolean");
    }
}

impl<'de> SimpleDeserializer<'de> for BoolDeserializer<'de> {}

impl<'de> RandomAccessDeserializer<'de> for BoolDeserializer<'de> {
    fn is_some(&self, idx: usize) -> Result<bool> {
        Ok(self.get(idx)?.is_some())
    }

    fn deserialize_any_some<V: Visitor<'de>>(&self, visitor: V, idx: usize) -> Result<V::Value> {
        self.deserialize_bool(visitor, idx)
    }

    fn deserialize_bool<V: Visitor<'de>>(&self, visitor: V, idx: usize) -> Result<V::Value> {
        try_(|| visitor.visit_bool::<Error>(self.get_required(idx)?)).ctx(self)
    }

    fn deserialize_u8<V: Visitor<'de>>(&self, visitor: V, idx: usize) -> Result<V::Value> {
        try_(|| visitor.visit_u8::<Error>(if self.get_required(idx)? { 1 } else { 0 })).ctx(self)
    }

    fn deserialize_u16<V: Visitor<'de>>(&self, visitor: V, idx: usize) -> Result<V::Value> {
        try_(|| visitor.visit_u16::<Error>(if self.get_required(idx)? { 1 } else { 0 })).ctx(self)
    }

    fn deserialize_u32<V: Visitor<'de>>(&self, visitor: V, idx: usize) -> Result<V::Value> {
        try_(|| visitor.visit_u32::<Error>(if self.get_required(idx)? { 1 } else { 0 })).ctx(self)
    }

    fn deserialize_u64<V: Visitor<'de>>(&self, visitor: V, idx: usize) -> Result<V::Value> {
        try_(|| visitor.visit_u64::<Error>(if self.get_required(idx)? { 1 } else { 0 })).ctx(self)
    }

    fn deserialize_i8<V: Visitor<'de>>(&self, visitor: V, idx: usize) -> Result<V::Value> {
        try_(|| visitor.visit_i8::<Error>(if self.get_required(idx)? { 1 } else { 0 })).ctx(self)
    }

    fn deserialize_i16<V: Visitor<'de>>(&self, visitor: V, idx: usize) -> Result<V::Value> {
        try_(|| visitor.visit_i16::<Error>(if self.get_required(idx)? { 1 } else { 0 })).ctx(self)
    }

    fn deserialize_i32<V: Visitor<'de>>(&self, visitor: V, idx: usize) -> Result<V::Value> {
        try_(|| visitor.visit_i32::<Error>(if self.get_required(idx)? { 1 } else { 0 })).ctx(self)
    }

    fn deserialize_i64<V: Visitor<'de>>(&self, visitor: V, idx: usize) -> Result<V::Value> {
        try_(|| visitor.visit_i64::<Error>(if self.get_required(idx)? { 1 } else { 0 })).ctx(self)
    }
}
