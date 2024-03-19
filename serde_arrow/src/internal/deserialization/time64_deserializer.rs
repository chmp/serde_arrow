use chrono::NaiveTime;
use serde::de::Visitor;

use crate::internal::{
    common::{BitBuffer, Mut},
    error::{fail, Result},
    schema::GenericTimeUnit,
};

use super::{simple_deserializer::SimpleDeserializer, utils::ArrayBufferIterator};

pub struct Time64Deserializer<'a>(ArrayBufferIterator<'a, i64>, i64, i64);

impl<'a> Time64Deserializer<'a> {
    pub fn new(buffer: &'a [i64], validity: Option<BitBuffer<'a>>, unit: GenericTimeUnit) -> Self {
        let (seconds_factor, nanoseconds_factor) = unit.get_factors();

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

impl<'de> SimpleDeserializer<'de> for Time64Deserializer<'de> {
    fn name() -> &'static str {
        "Time64Deserializer"
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
