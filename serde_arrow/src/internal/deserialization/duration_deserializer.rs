use marrow::{datatypes::TimeUnit, view::PrimitiveView};
use serde::de::Visitor;

use crate::internal::{
    chrono,
    error::{set_default, try_, Context, ContextSupport, Result},
    utils::{array_view_ext::ViewAccess, Mut},
};

use super::{
    random_access_deserializer::RandomAccessDeserializer, simple_deserializer::SimpleDeserializer,
    utils::ArrayBufferIterator,
};

pub struct DurationDeserializer<'a> {
    path: String,
    unit: TimeUnit,
    values: PrimitiveView<'a, i64>,
    array: ArrayBufferIterator<'a, i64>,
}

impl<'a> DurationDeserializer<'a> {
    pub fn new(path: String, unit: TimeUnit, view: PrimitiveView<'a, i64>) -> Self {
        Self {
            path,
            unit,
            values: view.clone(),
            array: ArrayBufferIterator::new(view.values, view.validity),
        }
    }

    pub fn next_string_value_required(&mut self) -> Result<String> {
        let value = self.array.next_required()?;
        Ok(chrono::format_arrow_duration_as_span(value, self.unit))
    }

    pub fn get_string_value(&self, idx: usize) -> Result<String> {
        let value = self.values.get_required(idx)?;
        Ok(chrono::format_arrow_duration_as_span(*value, self.unit))
    }
}

impl Context for DurationDeserializer<'_> {
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

impl<'de> RandomAccessDeserializer<'de> for DurationDeserializer<'de> {
    fn is_some(&self, idx: usize) -> Result<bool> {
        self.values.is_some(idx)
    }

    fn deserialize_any_some<V: Visitor<'de>>(&self, visitor: V, idx: usize) -> Result<V::Value> {
        self.deserialize_i64(visitor, idx)
    }

    fn deserialize_i64<V: Visitor<'de>>(&self, visitor: V, idx: usize) -> Result<V::Value> {
        try_(|| visitor.visit_i64(*self.values.get_required(idx)?)).ctx(self)
    }

    fn deserialize_str<V: Visitor<'de>>(&self, visitor: V, idx: usize) -> Result<V::Value> {
        try_(|| visitor.visit_str(self.get_string_value(idx)?.as_str())).ctx(self)
    }

    fn deserialize_string<V: Visitor<'de>>(&self, visitor: V, idx: usize) -> Result<V::Value> {
        try_(|| visitor.visit_string(self.get_string_value(idx)?)).ctx(self)
    }

    fn deserialize_bytes<V: Visitor<'de>>(&self, visitor: V, idx: usize) -> Result<V::Value> {
        try_(|| visitor.visit_bytes(self.get_string_value(idx)?.as_bytes())).ctx(self)
    }

    fn deserialize_byte_buf<V: Visitor<'de>>(&self, visitor: V, idx: usize) -> Result<V::Value> {
        try_(|| visitor.visit_byte_buf(self.get_string_value(idx)?.into_bytes())).ctx(self)
    }
}
