use half::f16;
use serde::de::Visitor;

use crate::internal::error::Result;

use super::{float_deserializer::Float, simple_deserializer::SimpleDeserializer};

impl Float for f16 {
    fn deserialize_any<'de, S: SimpleDeserializer<'de>, V: Visitor<'de>>(
        deser: &mut S,
        visitor: V,
    ) -> Result<V::Value> {
        deser.deserialize_f32(visitor)
    }

    fn into_f32(&self) -> Result<f32> {
        Ok(self.to_f32())
    }

    fn into_f64(&self) -> Result<f64> {
        Ok(self.to_f64())
    }
}

impl Float for f32 {
    fn deserialize_any<'de, S: SimpleDeserializer<'de>, V: Visitor<'de>>(
        deser: &mut S,
        visitor: V,
    ) -> Result<V::Value> {
        deser.deserialize_f32(visitor)
    }

    fn into_f32(&self) -> Result<f32> {
        Ok(*self)
    }

    fn into_f64(&self) -> Result<f64> {
        Ok(*self as f64)
    }
}

impl Float for f64 {
    fn deserialize_any<'de, S: SimpleDeserializer<'de>, V: Visitor<'de>>(
        deser: &mut S,
        visitor: V,
    ) -> Result<V::Value> {
        deser.deserialize_f64(visitor)
    }

    fn into_f32(&self) -> Result<f32> {
        Ok(*self as f32)
    }

    fn into_f64(&self) -> Result<f64> {
        Ok(*self)
    }
}
