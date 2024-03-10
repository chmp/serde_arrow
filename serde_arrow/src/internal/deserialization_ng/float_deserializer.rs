use serde::de::Visitor;

use crate::internal::{common::BitBuffer, error::Result, serialization_ng::utils::Mut};

use super::{simple_deserializer::SimpleDeserializer, utils::ArrayBufferIterator};

pub trait Float: Copy {
    fn deserialize_any<'de, S: SimpleDeserializer<'de>, V: Visitor<'de>>(
        deser: &mut S,
        visitor: V,
    ) -> Result<V::Value>;

    fn into_f32(&self) -> Result<f32>;
    fn into_f64(&self) -> Result<f64>;
}

pub struct FloatDeserializer<'a, F: Float>(ArrayBufferIterator<'a, F>);

impl<'a, F: Float> FloatDeserializer<'a, F> {
    pub fn new(buffer: &'a [F], validity: Option<BitBuffer<'a>>) -> Self {
        Self(ArrayBufferIterator::new(buffer, validity))
    }
}

impl<'de, F: Float> SimpleDeserializer<'de> for FloatDeserializer<'de, F> {
    fn name() -> &'static str {
        "FloatDeserializer"
    }

    fn deserialize_any<V: Visitor<'de>>(&mut self, visitor: V) -> Result<V::Value> {
        if self.0.peek_next()? {
            F::deserialize_any(self, visitor)
        } else {
            self.0.consume_next();
            visitor.visit_none()
        }
    }

    fn deserialize_option<V: Visitor<'de>>(&mut self, visitor: V) -> Result<V::Value> {
        if self.0.peek_next()? {
            visitor.visit_some(Mut(self))
        } else {
            self.0.consume_next();
            visitor.visit_none()
        }
    }

    fn deserialize_f32<V: Visitor<'de>>(&mut self, visitor: V) -> Result<V::Value> {
        visitor.visit_f32(self.0.next_required()?.into_f32()?)
    }

    fn deserialize_f64<V: Visitor<'de>>(&mut self, visitor: V) -> Result<V::Value> {
        visitor.visit_f64(self.0.next_required()?.into_f64()?)
    }
}
