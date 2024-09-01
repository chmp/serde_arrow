use serde::de::Visitor;

use crate::internal::{
    arrow::PrimitiveArrayView,
    error::{Context, Result},
    utils::{btree_map, Mut, NamedType},
};

use super::{simple_deserializer::SimpleDeserializer, utils::ArrayBufferIterator};

pub trait Float: Copy {
    fn deserialize_any<'de, S: SimpleDeserializer<'de>, V: Visitor<'de>>(
        deser: &mut S,
        visitor: V,
    ) -> Result<V::Value>;

    fn into_f32(self) -> Result<f32>;
    fn into_f64(self) -> Result<f64>;
}

pub struct FloatDeserializer<'a, F: Float> {
    path: String,
    array: ArrayBufferIterator<'a, F>,
}

impl<'a, F: Float> FloatDeserializer<'a, F> {
    pub fn new(path: String, view: PrimitiveArrayView<'a, F>) -> Self {
        Self {
            path,
            array: ArrayBufferIterator::new(view.values, view.validity),
        }
    }
}

impl<'de, F: NamedType + Float> Context for FloatDeserializer<'de, F> {
    fn annotations(&self) -> std::collections::BTreeMap<String, String> {
        let data_type = match F::NAME {
            "f16" => "Float16",
            "f32" => "Float32",
            "f64" => "Float64",
            _ => "<unknown>",
        };
        btree_map!("path" => self.path.clone(), "data_type" => data_type)
    }
}

impl<'de, F: NamedType + Float> SimpleDeserializer<'de> for FloatDeserializer<'de, F> {
    fn name() -> &'static str {
        "FloatDeserializer"
    }

    fn deserialize_any<V: Visitor<'de>>(&mut self, visitor: V) -> Result<V::Value> {
        if self.array.peek_next()? {
            F::deserialize_any(self, visitor)
        } else {
            self.array.consume_next();
            visitor.visit_none()
        }
    }

    fn deserialize_option<V: Visitor<'de>>(&mut self, visitor: V) -> Result<V::Value> {
        if self.array.peek_next()? {
            visitor.visit_some(Mut(self))
        } else {
            self.array.consume_next();
            visitor.visit_none()
        }
    }

    fn deserialize_f32<V: Visitor<'de>>(&mut self, visitor: V) -> Result<V::Value> {
        visitor.visit_f32(self.array.next_required()?.into_f32()?)
    }

    fn deserialize_f64<V: Visitor<'de>>(&mut self, visitor: V) -> Result<V::Value> {
        visitor.visit_f64(self.array.next_required()?.into_f64()?)
    }
}
