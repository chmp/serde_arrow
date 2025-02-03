use half::f16;
use serde::de::Visitor;

use crate::internal::error::Result;

use super::{float_deserializer::Float, random_access_deserializer::RandomAccessDeserializer};

impl Float for f16 {
    fn deserialize_any_at<'de, S: RandomAccessDeserializer<'de>, V: Visitor<'de>>(
        deser: &S,
        visitor: V,
        idx: usize,
    ) -> Result<V::Value> {
        deser.deserialize_f32(visitor, idx)
    }

    fn into_f32(self) -> Result<f32> {
        Ok(self.to_f32())
    }

    fn into_f64(self) -> Result<f64> {
        Ok(self.to_f64())
    }
}

impl Float for f32 {
    fn deserialize_any_at<'de, S: RandomAccessDeserializer<'de>, V: Visitor<'de>>(
        deser: &S,
        visitor: V,
        idx: usize,
    ) -> Result<V::Value> {
        deser.deserialize_f32(visitor, idx)
    }

    fn into_f32(self) -> Result<f32> {
        Ok(self)
    }

    fn into_f64(self) -> Result<f64> {
        Ok(self as f64)
    }
}

impl Float for f64 {
    fn deserialize_any_at<'de, S: RandomAccessDeserializer<'de>, V: Visitor<'de>>(
        deser: &S,
        visitor: V,
        idx: usize,
    ) -> Result<V::Value> {
        deser.deserialize_f64(visitor, idx)
    }

    fn into_f32(self) -> Result<f32> {
        Ok(self as f32)
    }

    fn into_f64(self) -> Result<f64> {
        Ok(self)
    }
}
