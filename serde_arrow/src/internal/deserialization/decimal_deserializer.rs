use serde::de::Visitor;

use crate::internal::{
    arrow::DecimalArrayView,
    error::{Context, Result},
    utils::{btree_map, decimal, Mut},
};

use super::{simple_deserializer::SimpleDeserializer, utils::ArrayBufferIterator};

pub struct DecimalDeserializer<'a> {
    path: String,
    inner: ArrayBufferIterator<'a, i128>,
    scale: i8,
}

impl<'a> DecimalDeserializer<'a> {
    pub fn new(path: String, view: DecimalArrayView<'a, i128>) -> Self {
        Self {
            path,
            inner: ArrayBufferIterator::new(view.values, view.validity),
            scale: view.scale,
        }
    }
}

impl<'de> Context for DecimalDeserializer<'de> {
    fn annotations(&self) -> std::collections::BTreeMap<String, String> {
        btree_map!("path" => self.path.clone(), "data_type" => "Decimal128(..)")
    }
}

impl<'de> SimpleDeserializer<'de> for DecimalDeserializer<'de> {
    fn name() -> &'static str {
        "DecimalDeserializer"
    }

    fn deserialize_any<V: Visitor<'de>>(&mut self, visitor: V) -> Result<V::Value> {
        if self.inner.peek_next()? {
            self.deserialize_str(visitor)
        } else {
            self.inner.consume_next();
            visitor.visit_none()
        }
    }

    fn deserialize_option<V: Visitor<'de>>(&mut self, visitor: V) -> Result<V::Value> {
        if self.inner.peek_next()? {
            visitor.visit_some(Mut(self))
        } else {
            self.inner.consume_next();
            visitor.visit_none()
        }
    }

    fn deserialize_str<V: Visitor<'de>>(&mut self, visitor: V) -> Result<V::Value> {
        let val = self.inner.next_required()?;
        let mut buffer = [0; decimal::BUFFER_SIZE_I128];
        let formatted = decimal::format_decimal(&mut buffer, val, self.scale);

        visitor.visit_str(formatted)
    }
}
