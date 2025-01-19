use marrow::view::DecimalView;
use serde::de::Visitor;

use crate::internal::{
    error::{set_default, try_, Context, ContextSupport, Result},
    utils::{decimal, Mut},
};

use super::{simple_deserializer::SimpleDeserializer, utils::ArrayBufferIterator};

pub struct DecimalDeserializer<'a> {
    path: String,
    inner: ArrayBufferIterator<'a, i128>,
    scale: i8,
}

impl<'a> DecimalDeserializer<'a> {
    pub fn new(path: String, view: DecimalView<'a, i128>) -> Self {
        Self {
            path,
            inner: ArrayBufferIterator::new(view.values, view.validity),
            scale: view.scale,
        }
    }
}

impl Context for DecimalDeserializer<'_> {
    fn annotate(&self, annotations: &mut std::collections::BTreeMap<String, String>) {
        set_default(annotations, "field", &self.path);
        set_default(annotations, "data_type", "Decimal128(..)");
    }
}

impl<'de> SimpleDeserializer<'de> for DecimalDeserializer<'de> {
    fn deserialize_any<V: Visitor<'de>>(&mut self, visitor: V) -> Result<V::Value> {
        try_(|| {
            if self.inner.peek_next()? {
                self.deserialize_str(visitor)
            } else {
                self.inner.consume_next();
                visitor.visit_none()
            }
        })
        .ctx(self)
    }

    fn deserialize_option<V: Visitor<'de>>(&mut self, visitor: V) -> Result<V::Value> {
        try_(|| {
            if self.inner.peek_next()? {
                visitor.visit_some(Mut(self))
            } else {
                self.inner.consume_next();
                visitor.visit_none()
            }
        })
        .ctx(self)
    }

    fn deserialize_str<V: Visitor<'de>>(&mut self, visitor: V) -> Result<V::Value> {
        try_(|| {
            let val = self.inner.next_required()?;
            let mut buffer = [0; decimal::BUFFER_SIZE_I128];
            let formatted = decimal::format_decimal(&mut buffer, val, self.scale);

            visitor.visit_str(formatted)
        })
        .ctx(self)
    }
}
