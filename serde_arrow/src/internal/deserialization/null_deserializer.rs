use serde::de::Visitor;

use crate::internal::error::{set_default, Context, ContextSupport, Error, Result};

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
    fn annotate(&self, annotations: &mut std::collections::BTreeMap<String, String>) {
        set_default(annotations, "field", &self.path);
        set_default(annotations, "data_type", "Null");
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
