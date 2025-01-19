use chrono::NaiveTime;
use marrow::{datatypes::TimeUnit, view::TimeView};
use serde::de::Visitor;

use crate::internal::{
    error::{fail, set_default, try_, Context, ContextSupport, Result},
    utils::{Mut, NamedType},
};

use super::{
    integer_deserializer::Integer, simple_deserializer::SimpleDeserializer,
    utils::ArrayBufferIterator,
};

pub struct TimeDeserializer<'a, T: Integer> {
    path: String,
    array: ArrayBufferIterator<'a, T>,
    seconds_factor: i64,
    nanoseconds_factor: i64,
}

impl<'a, T: Integer> TimeDeserializer<'a, T> {
    pub fn new(path: String, view: TimeView<'a, T>) -> Self {
        let (seconds_factor, nanoseconds_factor) = match view.unit {
            TimeUnit::Nanosecond => (1_000_000_000, 1),
            TimeUnit::Microsecond => (1_000_000, 1_000),
            TimeUnit::Millisecond => (1_000, 1_000_000),
            TimeUnit::Second => (1, 1_000_000_000),
        };

        Self {
            path,
            array: ArrayBufferIterator::new(view.values, view.validity),
            seconds_factor,
            nanoseconds_factor,
        }
    }

    pub fn get_string_repr(&self, ts: i64) -> Result<String> {
        let seconds = (ts / self.seconds_factor) as u32;
        let nanoseconds = ((ts % self.seconds_factor) / self.nanoseconds_factor) as u32;

        let Some(res) = NaiveTime::from_num_seconds_from_midnight_opt(seconds, nanoseconds) else {
            fail!("Invalid timestamp");
        };
        Ok(res.to_string())
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

impl<'de, T: NamedType + Integer> SimpleDeserializer<'de> for TimeDeserializer<'de, T> {
    fn deserialize_any<V: Visitor<'de>>(&mut self, visitor: V) -> Result<V::Value> {
        try_(|| {
            if self.array.peek_next()? {
                T::deserialize_any(&mut *self, visitor)
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
                visitor.visit_some(Mut(&mut *self))
            } else {
                self.array.consume_next();
                visitor.visit_none()
            }
        })
        .ctx(self)
    }

    fn deserialize_i32<V: Visitor<'de>>(&mut self, visitor: V) -> Result<V::Value> {
        try_(|| visitor.visit_i32(self.array.next_required()?.into_i32()?)).ctx(self)
    }

    fn deserialize_i64<V: Visitor<'de>>(&mut self, visitor: V) -> Result<V::Value> {
        try_(|| visitor.visit_i64(self.array.next_required()?.into_i64()?)).ctx(self)
    }

    fn deserialize_str<V: Visitor<'de>>(&mut self, visitor: V) -> Result<V::Value> {
        try_(|| self.deserialize_string(visitor)).ctx(self)
    }

    fn deserialize_string<V: Visitor<'de>>(&mut self, visitor: V) -> Result<V::Value> {
        try_(|| {
            let ts = self.array.next_required()?.into_i64()?;
            visitor.visit_string(self.get_string_repr(ts)?)
        })
        .ctx(self)
    }

    fn deserialize_bytes<V: Visitor<'de>>(&mut self, visitor: V) -> Result<V::Value> {
        try_(|| self.deserialize_byte_buf(visitor)).ctx(self)
    }

    fn deserialize_byte_buf<V: Visitor<'de>>(&mut self, visitor: V) -> Result<V::Value> {
        try_(|| {
            let ts = self.array.next_required()?.into_i64()?;
            visitor.visit_byte_buf(self.get_string_repr(ts)?.into_bytes())
        })
        .ctx(self)
    }
}
