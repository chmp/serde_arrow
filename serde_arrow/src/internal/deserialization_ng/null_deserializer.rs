use serde::de::Visitor;

use crate::Result;

use super::simple_deserializer::SimpleDeserializer;

pub struct NullDeserializer;

impl<'de> SimpleDeserializer<'de> for NullDeserializer {
    fn name() -> &'static str {
        "NullDeserializer"
    }

    fn deserialize_any<V: Visitor<'de>>(&mut self, visitor: V) -> Result<V::Value> {
        visitor.visit_unit()
    }

    fn deserialize_option<V: Visitor<'de>>(&mut self, visitor: V) -> Result<V::Value> {
        visitor.visit_none()
    }

    fn deserialize_unit<V: Visitor<'de>>(&mut self, visitor: V) -> Result<V::Value> {
        visitor.visit_unit()
    }

    fn deserialize_unit_struct<V: Visitor<'de>>(
        &mut self,
        _: &'static str,
        visitor: V,
    ) -> Result<V::Value> {
        visitor.visit_unit()
    }
}
