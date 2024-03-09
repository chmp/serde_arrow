use crate::{
    internal::{common::BitBuffer, error::fail, serialization_ng::utils::Mut},
    Result,
};

use super::{list_deserializer::IntoUsize, simple_deserializer::SimpleDeserializer};

pub struct StringDeserializer<'a, O: IntoUsize> {
    pub data: &'a [u8],
    pub offsets: &'a [O],
    pub validity: Option<BitBuffer<'a>>,
    pub next: usize,
}

impl<'a, O: IntoUsize> StringDeserializer<'a, O> {
    pub fn new(data: &'a [u8], offsets: &'a [O], validity: Option<BitBuffer<'a>>) -> Self {
        Self {
            data,
            offsets,
            validity,
            next: 0,
        }
    }

    pub fn next(&mut self) -> Result<Option<&str>> {
        if self.next + 1 > self.offsets.len() {
            fail!("Tried to deserialize a value from an exhausted StringDeserializer");
        }

        if let Some(validity) = &self.validity {
            if !validity.is_set(self.next) {
                return Ok(None);
            }
        }

        let start = self.offsets[self.next].into_usize()?;
        let end = self.offsets[self.next + 1].into_usize()?;
        let s = std::str::from_utf8(&self.data[start..end])?;

        self.next += 1;

        Ok(Some(s))
    }

    pub fn peek_next(&self) -> Result<bool> {
        if self.next + 1 > self.offsets.len() {
            fail!("Tried to deserialize a value from an exhausted StringDeserializer");
        }

        if let Some(validity) = &self.validity {
            if !validity.is_set(self.next) {
                return Ok(false);
            }
        }
        Ok(true)
    }

    pub fn consume_next(&mut self) {
        self.next += 1;
    }
}

impl<'a, O: IntoUsize> SimpleDeserializer<'a> for StringDeserializer<'a, O> {
    fn name() -> &'static str {
        "StringDeserializer"
    }

    fn deserialize_any<V: serde::de::Visitor<'a>>(&mut self, visitor: V) -> Result<V::Value> {
        if self.peek_next()? {
            self.deserialize_str(visitor)
        } else {
            self.consume_next();
            visitor.visit_none()
        }
    }

    fn deserialize_option<V: serde::de::Visitor<'a>>(&mut self, visitor: V) -> Result<V::Value> {
        if self.peek_next()? {
            visitor.visit_some(Mut(self))
        } else {
            self.consume_next();
            visitor.visit_none()
        }
    }

    fn deserialize_str<V: serde::de::Visitor<'a>>(&mut self, visitor: V) -> Result<V::Value> {
        let Some(s) = self.next()? else {
            fail!("Tried to deserialize a value from StringDeserializer, but value is missing");
        };
        visitor.visit_str(s)
    }

    fn deserialize_string<V: serde::de::Visitor<'a>>(&mut self, visitor: V) -> Result<V::Value> {
        let Some(s) = self.next()? else {
            fail!("Tried to deserialize a value from StringDeserializer, but value is missing");
        };
        visitor.visit_string(s.to_owned())
    }
}
