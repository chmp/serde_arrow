use half::f16;
use marrow::view::PrimitiveView;
use serde::de::Visitor;

use crate::internal::{
    error::{set_default, try_, Context, ContextSupport, Result},
    utils::{array_view_ext::ViewAccess, NamedType},
};

use super::random_access_deserializer::RandomAccessDeserializer;

pub trait Float: Copy {
    fn deserialize_any_at<'de, S: RandomAccessDeserializer<'de>, V: Visitor<'de>>(
        deser: &S,
        visitor: V,
        idx: usize,
    ) -> Result<V::Value>;

    fn into_f32(self) -> Result<f32>;
    fn into_f64(self) -> Result<f64>;
    fn into_string(self) -> Result<String>;
}

pub struct FloatDeserializer<'a, F: Float> {
    path: String,
    view: PrimitiveView<'a, F>,
}

impl<'a, F: Float> FloatDeserializer<'a, F> {
    pub fn new(path: String, view: PrimitiveView<'a, F>) -> Self {
        Self { path, view }
    }
}

impl<F: NamedType + Float> Context for FloatDeserializer<'_, F> {
    fn annotate(&self, annotations: &mut std::collections::BTreeMap<String, String>) {
        set_default(annotations, "field", &self.path);
        set_default(
            annotations,
            "data_type",
            match F::NAME {
                "f16" => "Float16",
                "f32" => "Float32",
                "f64" => "Float64",
                _ => "<unknown>",
            },
        );
    }
}

impl<'de, F: NamedType + Float> RandomAccessDeserializer<'de> for FloatDeserializer<'de, F> {
    fn is_some(&self, idx: usize) -> Result<bool> {
        self.view.is_some(idx)
    }

    fn deserialize_any_some<V: Visitor<'de>>(&self, visitor: V, idx: usize) -> Result<V::Value> {
        F::deserialize_any_at(self, visitor, idx)
    }

    fn deserialize_f32<V: Visitor<'de>>(&self, visitor: V, idx: usize) -> Result<V::Value> {
        try_(|| visitor.visit_f32(self.view.get_required(idx)?.into_f32()?)).ctx(self)
    }

    fn deserialize_f64<V: Visitor<'de>>(&self, visitor: V, idx: usize) -> Result<V::Value> {
        try_(|| visitor.visit_f64(self.view.get_required(idx)?.into_f64()?)).ctx(self)
    }

    fn deserialize_str<V: Visitor<'de>>(&self, visitor: V, idx: usize) -> Result<V::Value> {
        try_(|| visitor.visit_string(self.view.get_required(idx)?.into_string()?)).ctx(self)
    }

    fn deserialize_string<V: Visitor<'de>>(&self, visitor: V, idx: usize) -> Result<V::Value> {
        try_(|| visitor.visit_string(self.view.get_required(idx)?.into_string()?)).ctx(self)
    }
}

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

    fn into_string(self) -> Result<String> {
        Ok(self.to_f32().to_string())
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

    fn into_string(self) -> Result<String> {
        Ok(self.to_string())
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

    fn into_string(self) -> Result<String> {
        Ok(self.to_string())
    }
}
