use marrow::{types::MonthDayNanoInterval, view::PrimitiveView};
use serde::de::{value::MapDeserializer, Visitor};

use crate::internal::{
    chrono,
    error::{set_default, try_, Context, ContextSupport, Error, Result},
    utils::array_view_ext::ViewAccess,
};

use super::random_access_deserializer::RandomAccessDeserializer;

/// Deserialize `Interval(MonthDayNano)` arrays into span strings or into structs with the fields
/// `months`, `days` and `nanoseconds`
pub struct IntervalDeserializer<'a> {
    path: String,
    values: PrimitiveView<'a, MonthDayNanoInterval>,
}

impl<'a> IntervalDeserializer<'a> {
    pub fn new(path: String, values: PrimitiveView<'a, MonthDayNanoInterval>) -> Self {
        Self { path, values }
    }

    fn get_string_value(&self, idx: usize) -> Result<String> {
        chrono::format_arrow_interval_as_span(*self.values.get_required(idx)?)
    }

    fn visit_fields<'de, V: Visitor<'de>>(&self, visitor: V, idx: usize) -> Result<V::Value> {
        let value = self.values.get_required(idx)?;
        let fields = [
            ("months", i64::from(value.months)),
            ("days", i64::from(value.days)),
            ("nanoseconds", value.nanoseconds),
        ];
        let mut map = MapDeserializer::<_, Error>::new(fields.into_iter());
        let res = visitor.visit_map(&mut map)?;
        map.end()?;
        Ok(res)
    }
}

impl Context for IntervalDeserializer<'_> {
    fn annotate(&self, annotations: &mut std::collections::BTreeMap<String, String>) {
        set_default(annotations, "field", &self.path);
        set_default(annotations, "data_type", "Interval(MonthDayNano)");
    }
}

impl<'de> RandomAccessDeserializer<'de> for IntervalDeserializer<'de> {
    fn is_some(&self, idx: usize) -> Result<bool> {
        self.values.is_some(idx)
    }

    fn deserialize_any_some<V: Visitor<'de>>(&self, visitor: V, idx: usize) -> Result<V::Value> {
        try_(|| self.visit_fields(visitor, idx)).ctx(self)
    }

    fn deserialize_map<V: Visitor<'de>>(&self, visitor: V, idx: usize) -> Result<V::Value> {
        try_(|| self.visit_fields(visitor, idx)).ctx(self)
    }

    fn deserialize_struct<V: Visitor<'de>>(
        &self,
        _: &'static str,
        _: &'static [&'static str],
        visitor: V,
        idx: usize,
    ) -> Result<V::Value> {
        try_(|| self.visit_fields(visitor, idx)).ctx(self)
    }

    fn deserialize_str<V: Visitor<'de>>(&self, visitor: V, idx: usize) -> Result<V::Value> {
        try_(|| visitor.visit_str(self.get_string_value(idx)?.as_str())).ctx(self)
    }

    fn deserialize_string<V: Visitor<'de>>(&self, visitor: V, idx: usize) -> Result<V::Value> {
        try_(|| visitor.visit_string(self.get_string_value(idx)?)).ctx(self)
    }
}
