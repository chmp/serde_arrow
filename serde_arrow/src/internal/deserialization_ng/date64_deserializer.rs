use serde::de::Visitor;

use crate::{
    internal::{common::BitBuffer, error::fail, serialization_ng::utils::Mut},
    Result,
};

use super::{simple_deserializer::SimpleDeserializer, utils::ArrayBufferIterator};

pub struct Date64Deserializer<'a>(ArrayBufferIterator<'a, i64>, bool);

impl<'a> Date64Deserializer<'a> {
    pub fn new(buffer: &'a [i64], validity: Option<BitBuffer<'a>>, is_utc: bool) -> Self {
        Self(ArrayBufferIterator::new(buffer, validity), is_utc)
    }

    pub fn get_string_repr(&self, ts: i64) -> Result<String> {
        if self.1 {
            use chrono::{TimeZone, Utc};

            let Some(val) = Utc
                .timestamp_opt(ts / 1000, (ts % 1000) as u32 * 100_000)
                .earliest()
            else {
                fail!("Unsupported timestamp value: {ts}");
            };

            // NOTE: chrono documents that Debug, not Display, can be parsed
            Ok(format!("{:?}", val))
        } else {
            use chrono::NaiveDateTime;

            let Some(val) =
                NaiveDateTime::from_timestamp_opt(ts / 1000, (ts % 1000) as u32 * 100_000)
            else {
                fail!("Unsupported timestamp value: {ts}");
            };

            // NOTE: chrono documents that Debug, not Display, can be parsed
            Ok(format!("{:?}", val))
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
