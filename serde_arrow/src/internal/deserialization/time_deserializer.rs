use chrono::NaiveTime;
use marrow::{
    datatypes::TimeUnit,
    view::{PrimitiveView, TimeView},
};
use serde::de::Visitor;

use crate::internal::{
    error::{set_default, try_, try_opt, Context, ContextSupport, Error, Result},
    utils::{array_view_ext::ViewAccess, NamedType},
};

use super::{integer_deserializer::Integer, random_access_deserializer::RandomAccessDeserializer};

pub struct TimeDeserializer<'a, T: Integer> {
    path: String,
    values: PrimitiveView<'a, T>,
    unit: TimeUnit,
}

impl<'a, T: Integer> TimeDeserializer<'a, T> {
    pub fn new(path: String, view: TimeView<'a, T>) -> Self {
        Self {
            path,
            values: PrimitiveView {
                validity: view.validity,
                values: view.values,
            },
            unit: view.unit,
        }
    }

    pub fn get_string_repr(&self, ts: i64) -> Result<String> {
        try_opt(|| {
            let (secs, nano) = match self.unit {
                TimeUnit::Second => (ts, 0),
                TimeUnit::Millisecond => (ts / 1_000, (ts % 1_000) * 1_000_000),
                TimeUnit::Microsecond => (ts / 1_000_000, (ts % 1_000_000) * 1_000),
                TimeUnit::Nanosecond => (ts / 1_000_000_000, ts % 1_000_000_000),
            };
            let time = NaiveTime::from_num_seconds_from_midnight_opt(
                u32::try_from(secs).ok()?,
                u32::try_from(nano).ok()?,
            )?;
            Some(time.to_string())
        })
        .ok_or_else(|| {
            Error::custom(format!(
                "Cannot convert {ts} into Time64({unit})",
                unit = self.unit
            ))
        })
    }
}

impl<T: NamedType + Integer> Context for TimeDeserializer<'_, T> {
    fn annotate(&self, annotations: &mut std::collections::BTreeMap<String, String>) {
        set_default(annotations, "field", &self.path);
        set_default(
            annotations,
            "data_type",
            match T::NAME {
                "i32" => "Time32",
                "i64" => "Time64",
                _ => "<unknown>",
            },
        );
    }
}

impl<'de, T: Integer + NamedType> RandomAccessDeserializer<'de> for TimeDeserializer<'de, T> {
    fn is_some(&self, idx: usize) -> Result<bool> {
        self.values.is_some(idx)
    }

    fn deserialize_any_some<V: Visitor<'de>>(&self, visitor: V, idx: usize) -> Result<V::Value> {
        T::deserialize_any_at(self, visitor, idx)
    }

    fn deserialize_i32<V: Visitor<'de>>(&self, visitor: V, idx: usize) -> Result<V::Value> {
        try_(|| visitor.visit_i32(self.values.get_required(idx)?.into_i32()?)).ctx(self)
    }

    fn deserialize_i64<V: Visitor<'de>>(&self, visitor: V, idx: usize) -> Result<V::Value> {
        try_(|| visitor.visit_i64(self.values.get_required(idx)?.into_i64()?)).ctx(self)
    }

    fn deserialize_str<V: Visitor<'de>>(&self, visitor: V, idx: usize) -> Result<V::Value> {
        try_(|| self.deserialize_string(visitor, idx)).ctx(self)
    }

    fn deserialize_string<V: Visitor<'de>>(&self, visitor: V, idx: usize) -> Result<V::Value> {
        try_(|| {
            let ts = self.values.get_required(idx)?.into_i64()?;
            visitor.visit_string(self.get_string_repr(ts)?)
        })
        .ctx(self)
    }

    fn deserialize_bytes<V: Visitor<'de>>(&self, visitor: V, idx: usize) -> Result<V::Value> {
        try_(|| self.deserialize_byte_buf(visitor, idx)).ctx(self)
    }

    fn deserialize_byte_buf<V: Visitor<'de>>(&self, visitor: V, idx: usize) -> Result<V::Value> {
        try_(|| {
            let ts = self.values.get_required(idx)?.into_i64()?;
            visitor.visit_byte_buf(self.get_string_repr(ts)?.into_bytes())
        })
        .ctx(self)
    }
}
