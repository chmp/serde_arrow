use chrono::{DateTime, Datelike, Utc};
use marrow::{
    datatypes::TimeUnit,
    view::{PrimitiveView, TimestampView},
};
use serde::de::Visitor;

use crate::internal::{
    error::{fail, set_default, try_, Context, ContextSupport, Result},
    utils::array_view_ext::ViewAccess,
};

use super::random_access_deserializer::RandomAccessDeserializer;

pub struct TimestampDeserializer<'a> {
    path: String,
    values: PrimitiveView<'a, i64>,
    unit: TimeUnit,
    is_utc: bool,
}

impl<'a> TimestampDeserializer<'a> {
    pub fn new(path: String, view: TimestampView<'a>) -> Result<Self> {
        Ok(Self {
            path,
            values: PrimitiveView {
                validity: view.validity,
                values: view.values,
            },
            unit: view.unit,
            is_utc: is_utc_timestamp(view.timezone.as_deref())?,
        })
    }

    pub fn get_string_repr(&self, ts: i64) -> Result<String> {
        let Some(date_time) = (match self.unit {
            TimeUnit::Second => DateTime::from_timestamp(ts, 0),
            TimeUnit::Millisecond => DateTime::from_timestamp_millis(ts),
            TimeUnit::Microsecond => DateTime::from_timestamp_micros(ts),
            TimeUnit::Nanosecond => Some(DateTime::from_timestamp_nanos(ts)),
        }) else {
            fail!("Unsupported timestamp value: {ts}");
        };

        if self.is_utc {
            Ok(self.format_with_suffix(date_time, "Z"))
        } else {
            Ok(self.format_with_suffix(date_time, ""))
        }
    }

    pub fn format_with_suffix(&self, date_time: DateTime<Utc>, suffix: &str) -> String {
        let date_time = date_time.naive_utc();
        // special handling of negative dates:
        //
        // - jiff expects 6 digits years in this case
        // - chrono allows an arbitrary number of digits, when prefixed with a sign
        //
        // https://github.com/chronotope/chrono/blob/05a6ce68cf18a01274cef211b080a7170c7c1a1f/src/format/parse.rs#L368
        if date_time.year() < 0 {
            // NOTE: chrono documents that Debug, not Display, can be parsed
            format!(
                "-{positive_year:06}-{month:02}-{day:02}T{time:?}{suffix}",
                positive_year = -date_time.year(),
                month = date_time.month(),
                day = date_time.day(),
                time = date_time.time(),
            )
        } else {
            // NOTE: chrono documents that Debug, not Display, can be parsed
            format!("{:?}{suffix}", date_time)
        }
    }
}

fn is_utc_timestamp(timezone: Option<&str>) -> Result<bool> {
    match timezone {
        Some(tz) if tz.to_lowercase() == "utc" => Ok(true),
        Some(tz) => fail!("Unsupported timezone: {} is not supported", tz),
        None => Ok(false),
    }
}

impl Context for TimestampDeserializer<'_> {
    fn annotate(&self, annotations: &mut std::collections::BTreeMap<String, String>) {
        set_default(annotations, "field", &self.path);
        set_default(annotations, "data_type", "Date64");
    }
}

impl<'de> RandomAccessDeserializer<'de> for TimestampDeserializer<'de> {
    fn is_some(&self, idx: usize) -> Result<bool> {
        self.values.is_some(idx)
    }

    fn deserialize_any_some<V: Visitor<'de>>(&self, visitor: V, idx: usize) -> Result<V::Value> {
        self.deserialize_i64(visitor, idx)
    }

    fn deserialize_i64<V: Visitor<'de>>(&self, visitor: V, idx: usize) -> Result<V::Value> {
        try_(|| visitor.visit_i64(*self.values.get_required(idx)?)).ctx(self)
    }

    fn deserialize_str<V: Visitor<'de>>(&self, visitor: V, idx: usize) -> Result<V::Value> {
        try_(|| self.deserialize_string(visitor, idx)).ctx(self)
    }

    fn deserialize_string<V: Visitor<'de>>(&self, visitor: V, idx: usize) -> Result<V::Value> {
        try_(|| {
            let ts = self.values.get_required(idx)?;
            visitor.visit_string(self.get_string_repr(*ts)?)
        })
        .ctx(self)
    }

    fn deserialize_bytes<V: Visitor<'de>>(&self, visitor: V, idx: usize) -> Result<V::Value> {
        try_(|| self.deserialize_byte_buf(visitor, idx).ctx(self))
    }

    fn deserialize_byte_buf<V: Visitor<'de>>(&self, visitor: V, idx: usize) -> Result<V::Value> {
        try_(|| {
            let ts = self.values.get_required(idx)?;
            visitor.visit_byte_buf(self.get_string_repr(*ts)?.into_bytes())
        })
        .ctx(self)
    }
}
