use chrono::DateTime;
use serde::de::Visitor;

use crate::internal::{
    arrow::{BitsWithOffset, TimeUnit},
    error::{fail, Result},
    utils::Mut,
};

use super::{simple_deserializer::SimpleDeserializer, utils::ArrayBufferIterator};

pub struct Date64Deserializer<'a>(ArrayBufferIterator<'a, i64>, TimeUnit, bool);

impl<'a> Date64Deserializer<'a> {
    pub fn new(
        buffer: &'a [i64],
        validity: Option<BitsWithOffset<'a>>,
        unit: TimeUnit,
        is_utc: bool,
    ) -> Self {
        Self(ArrayBufferIterator::new(buffer, validity), unit, is_utc)
    }

    pub fn get_string_repr(&self, ts: i64) -> Result<String> {
        let Some(date_time) = (match self.1 {
            TimeUnit::Second => DateTime::from_timestamp(ts, 0),
            TimeUnit::Millisecond => DateTime::from_timestamp_millis(ts),
            TimeUnit::Microsecond => DateTime::from_timestamp_micros(ts),
            TimeUnit::Nanosecond => Some(DateTime::from_timestamp_nanos(ts)),
        }) else {
            fail!("Unsupported timestamp value: {ts}");
        };

        if self.2 {
            // NOTE: chrono documents that Debug, not Display, can be parsed
            Ok(format!("{:?}", date_time))
        } else {
            // NOTE: chrono documents that Debug, not Display, can be parsed
            Ok(format!("{:?}", date_time.naive_utc()))
        }
    }
}

impl<'de> SimpleDeserializer<'de> for Date64Deserializer<'de> {
    fn name() -> &'static str {
        "Date64Deserializer"
    }

    fn deserialize_any<V: Visitor<'de>>(&mut self, visitor: V) -> Result<V::Value> {
        if self.0.peek_next()? {
            self.deserialize_i64(visitor)
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

    fn deserialize_i64<V: Visitor<'de>>(&mut self, visitor: V) -> Result<V::Value> {
        visitor.visit_i64(self.0.next_required()?)
    }

    fn deserialize_str<V: Visitor<'de>>(&mut self, visitor: V) -> Result<V::Value> {
        self.deserialize_string(visitor)
    }

    fn deserialize_string<V: Visitor<'de>>(&mut self, visitor: V) -> Result<V::Value> {
        let ts = self.0.next_required()?;
        visitor.visit_string(self.get_string_repr(ts)?)
    }
}
