use marrow::view::{DecimalView, PrimitiveView};
use serde::de::Visitor;

use crate::internal::{
    error::{set_default, try_, Context, ContextSupport, Result},
    utils::{array_view_ext::ViewAccess, decimal, Mut},
};

use super::{
    random_access_deserializer::RandomAccessDeserializer, simple_deserializer::SimpleDeserializer,
    utils::ArrayBufferIterator,
};

pub struct DecimalDeserializer<'a> {
    path: String,
    view: PrimitiveView<'a, i128>,
    inner: ArrayBufferIterator<'a, i128>,
    scale: i8,
}

impl<'a> DecimalDeserializer<'a> {
    pub fn new(path: String, view: DecimalView<'a, i128>) -> Self {
        Self {
            path,
            view: PrimitiveView {
                validity: view.validity,
                values: view.values,
            },
            inner: ArrayBufferIterator::new(view.values, view.validity),
            scale: view.scale,
        }
    }

    fn with_next_value<F: FnOnce(&str) -> Result<R>, R>(&mut self, func: F) -> Result<R> {
        let val = self.inner.next_required()?;
        let mut buffer = [0; decimal::BUFFER_SIZE_I128];
        let formatted = decimal::format_decimal(&mut buffer, val, self.scale);

        func(formatted)
    }

    fn with_value<F: FnOnce(&str) -> Result<R>, R>(&self, idx: usize, func: F) -> Result<R> {
        let val = self.view.get_required(idx)?;
        let mut buffer = [0; decimal::BUFFER_SIZE_I128];
        let formatted = decimal::format_decimal(&mut buffer, *val, self.scale);

        func(formatted)
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
        self.with_next_value(|value| visitor.visit_str(value))
            .ctx(self)
    }

    fn deserialize_string<V: Visitor<'de>>(&mut self, visitor: V) -> Result<V::Value> {
        self.with_next_value(|value| visitor.visit_string(value.to_string()))
            .ctx(self)
    }
}

impl<'de> RandomAccessDeserializer<'de> for DecimalDeserializer<'de> {
    fn is_some(&self, idx: usize) -> Result<bool> {
        self.view.is_some(idx)
    }

    fn deserialize_any_some<V: Visitor<'de>>(&self, visitor: V, idx: usize) -> Result<V::Value> {
        self.deserialize_str(visitor, idx)
    }

    fn deserialize_str<V: Visitor<'de>>(&self, visitor: V, idx: usize) -> Result<V::Value> {
        self.with_value(idx, |value| visitor.visit_str(value))
            .ctx(self)
    }

    fn deserialize_string<V: Visitor<'de>>(&self, visitor: V, idx: usize) -> Result<V::Value> {
        self.with_value(idx, |value| visitor.visit_string(value.to_string()))
            .ctx(self)
    }
}
