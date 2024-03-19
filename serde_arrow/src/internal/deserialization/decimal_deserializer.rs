use serde::de::Visitor;

use crate::internal::{
    common::{BitBuffer, Mut},
    decimal,
    error::Result,
};

use super::{simple_deserializer::SimpleDeserializer, utils::ArrayBufferIterator};

pub struct DecimalDeserializer<'a> {
    inner: ArrayBufferIterator<'a, i128>,
    scale: i8,
}

impl<'a> DecimalDeserializer<'a> {
    pub fn new(buffer: &'a [i128], validity: Option<BitBuffer<'a>>, scale: i8) -> Self {
        Self {
            inner: ArrayBufferIterator::new(buffer, validity),
            scale,
        }
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
