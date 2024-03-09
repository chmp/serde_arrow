use serde::de::Visitor;

use crate::{
    internal::{
        common::BitBuffer,
        error::{error, fail},
        serialization_ng::utils::Mut,
    },
    Result,
};

use super::simple_deserializer::SimpleDeserializer;

pub trait Float: Copy {
    fn deserialize_any<'de, S: SimpleDeserializer<'de>, V: Visitor<'de>>(
        deser: &mut S,
        visitor: V,
    ) -> Result<V::Value>;

    fn into_f32(&self) -> Result<f32>;
    fn into_f64(&self) -> Result<f64>;
}

pub struct FloatDeserializer<'a, F: Float> {
    pub buffer: &'a [F],
    pub validity: Option<BitBuffer<'a>>,
    pub next: usize,
}

impl<'a, F: Float> FloatDeserializer<'a, F> {
    pub fn new(buffer: &'a [F], validity: Option<BitBuffer<'a>>) -> Self {
        Self {
            buffer,
            validity,
            next: 0,
        }
    }

    pub fn next(&mut self) -> Result<Option<F>> {
        if self.next > self.buffer.len() {
            fail!("Tried to deserialize a value from an exhausted FloatDeserializer");
        }

        if let Some(validity) = &self.validity {
            if !validity.is_set(self.next) {
                return Ok(None);
            }
        }
        let val = self.buffer[self.next];
        self.next += 1;

        Ok(Some(val))
    }

    pub fn next_required(&mut self) -> Result<F> {
        self.next()?.ok_or_else(|| error!("missing value"))
    }

    pub fn peek_next(&self) -> Result<bool> {
        if self.next > self.buffer.len() {
            fail!("Tried to deserialize a value from an exhausted StringDeserializer");
        }

        if let Some(validity) = &self.validity {
            if !validity.is_set(self.next) {
                return Ok(false);
            }
        }
        Ok(true)
    }

    pub fn consume_next(&mut self) {
        self.next += 1;
    }
}

impl<'de, F: Float> SimpleDeserializer<'de> for FloatDeserializer<'de, F> {
    fn name() -> &'static str {
        "FloatDeserializer"
    }

    fn deserialize_any<V: Visitor<'de>>(&mut self, visitor: V) -> Result<V::Value> {
        if self.peek_next()? {
            F::deserialize_any(self, visitor)
        } else {
            self.consume_next();
            visitor.visit_none()
        }
    }

    fn deserialize_option<V: Visitor<'de>>(&mut self, visitor: V) -> Result<V::Value> {
        if self.peek_next()? {
            visitor.visit_some(Mut(self))
        } else {
            self.consume_next();
            visitor.visit_none()
        }
    }

    fn deserialize_f32<V: Visitor<'de>>(&mut self, visitor: V) -> Result<V::Value> {
        visitor.visit_f32(self.next_required()?.into_f32()?)
    }

    fn deserialize_f64<V: Visitor<'de>>(&mut self, visitor: V) -> Result<V::Value> {
        visitor.visit_f64(self.next_required()?.into_f64()?)
    }
}
