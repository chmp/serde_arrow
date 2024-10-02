use chrono::{DateTime, Datelike, Utc};
use serde::de::Visitor;

use crate::internal::{
    arrow::{BitsWithOffset, TimeUnit},
    error::{fail, set_default, try_, Context, ContextSupport, Result},
    utils::Mut,
};

use super::{simple_deserializer::SimpleDeserializer, utils::ArrayBufferIterator};

pub struct Date64Deserializer<'a> {
    path: String,
    array: ArrayBufferIterator<'a, i64>,
    unit: TimeUnit,
    is_utc: bool,
}

impl<'a> Date64Deserializer<'a> {
    pub fn new(
        path: String,
        buffer: &'a [i64],
        validity: Option<BitsWithOffset<'a>>,
        unit: TimeUnit,
        is_utc: bool,
    ) -> Self {
        Self {
            path,
            array: ArrayBufferIterator::new(buffer, validity),
            unit,
            is_utc,
        }
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
            Ok(self.format_utc(date_time))
        } else {
            Ok(self.format_non_utc(date_time))
        }
    }

    pub fn format_utc(&self, date_time: DateTime<Utc>) -> String {
        // NOTE: chrono documents that Debug, not Display, can be parsed
        format!("{:?}", date_time)
    }

    pub fn format_non_utc(&self, date_time: DateTime<Utc>) -> String {
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
                "-{positive_year:06}-{month:02}-{day:02}T{time:?}",
                positive_year = -date_time.year(),
                month = date_time.month(),
                day = date_time.day(),
                time = date_time.time(),
            )
        } else {
            // NOTE: chrono documents that Debug, not Display, can be parsed
            format!("{:?}", date_time)
        }
    }
}

impl<'de> Context for Date64Deserializer<'de> {
    fn annotate(&self, annotations: &mut std::collections::BTreeMap<String, String>) {
        set_default(annotations, "field", &self.path);
        set_default(annotations, "data_type", "Date64");
    }
}

impl<'de> SimpleDeserializer<'de> for Date64Deserializer<'de> {
    fn deserialize_any<V: Visitor<'de>>(&mut self, visitor: V) -> Result<V::Value> {
        try_(|| {
            if self.array.peek_next()? {
                self.deserialize_i64(visitor)
            } else {
                self.array.consume_next();
                visitor.visit_none()
            }
        })
        .ctx(self)
    }

    fn deserialize_option<V: Visitor<'de>>(&mut self, visitor: V) -> Result<V::Value> {
        try_(|| {
            if self.array.peek_next()? {
                visitor.visit_some(Mut(self))
            } else {
                self.array.consume_next();
                visitor.visit_none()
            }
        })
        .ctx(self)
    }

    fn deserialize_i64<V: Visitor<'de>>(&mut self, visitor: V) -> Result<V::Value> {
        try_(|| visitor.visit_i64(self.array.next_required()?)).ctx(self)
    }

    fn deserialize_str<V: Visitor<'de>>(&mut self, visitor: V) -> Result<V::Value> {
        try_(|| self.deserialize_string(visitor)).ctx(self)
    }

    fn deserialize_string<V: Visitor<'de>>(&mut self, visitor: V) -> Result<V::Value> {
        try_(|| {
            let ts = self.array.next_required()?;
            visitor.visit_string(self.get_string_repr(ts)?)
        })
        .ctx(self)
    }

    fn deserialize_bytes<V: Visitor<'de>>(&mut self, visitor: V) -> Result<V::Value> {
        try_(|| self.deserialize_byte_buf(visitor).ctx(self))
    }

    fn deserialize_byte_buf<V: Visitor<'de>>(&mut self, visitor: V) -> Result<V::Value> {
        try_(|| {
            let ts = self.array.next_required()?;
            visitor.visit_byte_buf(self.get_string_repr(ts)?.into_bytes())
        })
        .ctx(self)
    }
}
