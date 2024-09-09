use serde::de::Visitor;

use crate::internal::{
    error::{Context, ContextSupport, Error, Result},
    utils::btree_map,
};

use super::simple_deserializer::SimpleDeserializer;

pub struct NullDeserializer {
    path: String,
}

impl NullDeserializer {
    pub fn new(path: String) -> Self {
        Self { path }
    }
}

impl Context for NullDeserializer {
    fn annotations(&self) -> std::collections::BTreeMap<String, String> {
        btree_map!("field" => self.path.clone(), "data_type" => "Null")
    }
}

impl<'de> SimpleDeserializer<'de> for NullDeserializer {
    fn deserialize_any<V: Visitor<'de>>(&mut self, visitor: V) -> Result<V::Value> {
        visitor.visit_unit::<Error>().ctx(self)
    }

    fn deserialize_option<V: Visitor<'de>>(&mut self, visitor: V) -> Result<V::Value> {
        visitor.visit_none::<Error>().ctx(self)
    }

    fn deserialize_unit<V: Visitor<'de>>(&mut self, visitor: V) -> Result<V::Value> {
        visitor.visit_unit::<Error>().ctx(self)
    }

    fn deserialize_unit_struct<V: Visitor<'de>>(
        &mut self,
        _: &'static str,
        visitor: V,
    ) -> Result<V::Value> {
        visitor.visit_unit::<Error>().ctx(self)
    }
}
