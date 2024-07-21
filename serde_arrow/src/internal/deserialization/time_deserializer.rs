use chrono::NaiveTime;
use serde::de::Visitor;

use crate::internal::{
    arrow::TimeUnit,
    error::{fail, Result},
    utils::Mut,
};

use super::{
    integer_deserializer::Integer,
    simple_deserializer::SimpleDeserializer,
    utils::{ArrayBufferIterator, BitBuffer},
};

pub struct TimeDeserializer<'a, T: Integer>(ArrayBufferIterator<'a, T>, i64, i64);

impl<'a, T: Integer> TimeDeserializer<'a, T> {
    pub fn new(buffer: &'a [T], validity: Option<BitBuffer<'a>>, unit: TimeUnit) -> Self {
        let (seconds_factor, nanoseconds_factor) = match unit {
            TimeUnit::Nanosecond => (1_000_000_000, 1),
            TimeUnit::Microsecond => (1_000_000, 1_000),
            TimeUnit::Millisecond => (1_000, 1_000_000),
            TimeUnit::Second => (1, 1_000_000_000),
        };

        Self(
            ArrayBufferIterator::new(buffer, validity),
            seconds_factor,
            nanoseconds_factor,
        )
    }

    pub fn get_string_repr(&self, ts: i64) -> Result<String> {
        let seconds = (ts / self.1) as u32;
        let nanoseconds = ((ts % self.1) / self.2) as u32;

        let Some(res) = NaiveTime::from_num_seconds_from_midnight_opt(seconds, nanoseconds) else {
            fail!("Invalid timestamp");
        };
        Ok(res.to_string())
    }
}

impl<'de, T: Integer> SimpleDeserializer<'de> for TimeDeserializer<'de, T> {
    fn name() -> &'static str {
        "Time64Deserializer"
    }

    fn deserialize_any<V: Visitor<'de>>(&mut self, visitor: V) -> Result<V::Value> {
        if self.0.peek_next()? {
            T::deserialize_any(self, visitor)
        } else {
            self.0.consume_next();
            visitor.visit_none()
        }
    }

    fn deserialize_option<V: Visitor<'de>>(&mut self, visitor: V) -> Result<V::Value> {
        if self.0.peek_next()? {
            visitor.visit_some(Mut(self))
        } else {
            self.0.consume_next();
            visitor.visit_none()
        }
    }

    fn deserialize_i32<V: Visitor<'de>>(&mut self, visitor: V) -> Result<V::Value> {
        visitor.visit_i32(self.0.next_required()?.into_i32()?)
    }

    fn deserialize_i64<V: Visitor<'de>>(&mut self, visitor: V) -> Result<V::Value> {
        visitor.visit_i64(self.0.next_required()?.into_i64()?)
    }

    fn deserialize_str<V: Visitor<'de>>(&mut self, visitor: V) -> Result<V::Value> {
        self.deserialize_string(visitor)
    }

    fn deserialize_string<V: Visitor<'de>>(&mut self, visitor: V) -> Result<V::Value> {
        let ts = self.0.next_required()?.into_i64()?;
        visitor.visit_string(self.get_string_repr(ts)?)
    }
}
