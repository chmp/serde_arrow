use marrow::{
    types::i256,
    view::{DecimalView, PrimitiveView},
};
use serde::de::Visitor;

use crate::internal::{
    error::{set_default, Context, ContextSupport, Result},
    utils::{array_view_ext::ViewAccess, decimal},
};

use super::random_access_deserializer::RandomAccessDeserializer;

pub trait DecimalPrimitive: Copy + 'static {
    const DATA_TYPE_NAME: &'static str;

    fn with_formatted<F: FnOnce(&str) -> Result<R>, R>(self, scale: i8, func: F) -> Result<R>;
}

impl DecimalPrimitive for i128 {
    const DATA_TYPE_NAME: &'static str = "Decimal128";

    fn with_formatted<F: FnOnce(&str) -> Result<R>, R>(self, scale: i8, func: F) -> Result<R> {
        let mut buffer = [0; decimal::BUFFER_SIZE_I128];
        func(decimal::format_decimal(&mut buffer, self, scale)?)
    }
}

impl DecimalPrimitive for i256 {
    const DATA_TYPE_NAME: &'static str = "Decimal256";

    fn with_formatted<F: FnOnce(&str) -> Result<R>, R>(self, scale: i8, func: F) -> Result<R> {
        let mut buffer = [0; decimal::BUFFER_SIZE_I256];
        func(decimal::format_decimal256(&mut buffer, self, scale)?)
    }
}

pub struct DecimalDeserializer<'a, D: DecimalPrimitive> {
    path: String,
    view: PrimitiveView<'a, D>,
    precision: u8,
    scale: i8,
}

impl<'a, D: DecimalPrimitive> DecimalDeserializer<'a, D> {
    pub fn new(path: String, view: DecimalView<'a, D>) -> Self {
        Self {
            path,
            view: PrimitiveView {
                validity: view.validity,
                values: view.values,
            },
            precision: view.precision,
            scale: view.scale,
        }
    }

    fn with_value<F: FnOnce(&str) -> Result<R>, R>(&self, idx: usize, func: F) -> Result<R> {
        let val = self.view.get_required(idx)?;
        val.with_formatted(self.scale, func)
    }
}

impl<D: DecimalPrimitive> Context for DecimalDeserializer<'_, D> {
    fn annotate(&self, annotations: &mut std::collections::BTreeMap<String, String>) {
        set_default(annotations, "field", &self.path);
        set_default(
            annotations,
            "data_type",
            format!(
                "{name}({precision}, {scale})",
                name = D::DATA_TYPE_NAME,
                precision = self.precision,
                scale = self.scale
            ),
        );
    }
}

impl<'de, D: DecimalPrimitive> RandomAccessDeserializer<'de> for DecimalDeserializer<'de, D> {
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
