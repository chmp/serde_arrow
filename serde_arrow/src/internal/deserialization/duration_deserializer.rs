use serde::de::Visitor;

use crate::internal::{
    arrow::{PrimitiveArrayView, TimeUnit},
    chrono,
    error::{set_default, try_, Context, ContextSupport, Result},
    utils::Mut,
};

use super::{simple_deserializer::SimpleDeserializer, utils::ArrayBufferIterator};

pub struct DurationDeserializer<'a> {
    path: String,
    unit: TimeUnit,
    array: ArrayBufferIterator<'a, i64>,
}

impl<'a> DurationDeserializer<'a> {
    pub fn new(path: String, unit: TimeUnit, view: PrimitiveArrayView<'a, i64>) -> Self {
        Self {
            path,
            unit,
            array: ArrayBufferIterator::new(view.values, view.validity),
        }
    }

    pub fn next_string_value_required(&mut self) -> Result<String> {
        let value = self.array.next_required()?;
        Ok(chrono::format_arrow_duration_as_span(value, self.unit))
    }
}

impl<'de> Context for DurationDeserializer<'de> {
    fn annotate(&self, annotations: &mut std::collections::BTreeMap<String, String>) {
        set_default(annotations, "field", &self.path);
        set_default(annotations, "data_type", "Duration(..)");
    }
}

impl<'de> SimpleDeserializer<'de> for DurationDeserializer<'de> {
    fn deserialize_any<V: Visitor<'de>>(&mut self, visitor: V) -> Result<V::Value> {
        try_(|| {
            if self.array.peek_next()? {
                self.deserialize_i64(visitor)
            } else {
                self.array.consume_next();
                visitor.visit_none()
            }
        })
        .ctx(self)
    }

    fn deserialize_option<V: Visitor<'de>>(&mut self, visitor: V) -> Result<V::Value> {
        try_(|| {
            if self.array.peek_next()? {
                visitor.visit_some(Mut(&mut *self))
            } else {
                self.array.consume_next();
                visitor.visit_none()
            }
        })
        .ctx(self)
    }

    fn deserialize_i64<V: Visitor<'de>>(&mut self, visitor: V) -> Result<V::Value> {
        try_(|| visitor.visit_i64(self.array.next_required()?)).ctx(self)
    }

    fn deserialize_str<V: Visitor<'de>>(&mut self, visitor: V) -> Result<V::Value> {
        try_(|| visitor.visit_str(self.next_string_value_required()?.as_str())).ctx(self)
    }

    fn deserialize_string<V: Visitor<'de>>(&mut self, visitor: V) -> Result<V::Value> {
        try_(|| visitor.visit_string(self.next_string_value_required()?)).ctx(self)
    }

    fn deserialize_bytes<V: Visitor<'de>>(&mut self, visitor: V) -> Result<V::Value> {
        try_(|| visitor.visit_bytes(self.next_string_value_required()?.as_bytes())).ctx(self)
    }

    fn deserialize_byte_buf<V: Visitor<'de>>(&mut self, visitor: V) -> Result<V::Value> {
        try_(|| visitor.visit_byte_buf(self.next_string_value_required()?.into_bytes())).ctx(self)
    }
}
