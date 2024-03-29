use serde::de::Visitor;

use crate::internal::{
    common::{BitBuffer, Mut},
    error::{fail, Result},
};

use super::{
    integer_deserializer::Integer, list_deserializer::IntoUsize,
    simple_deserializer::SimpleDeserializer, utils::ArrayBufferIterator,
};

pub struct DictionaryDeserializer<'a, K: Integer, V: IntoUsize> {
    keys: ArrayBufferIterator<'a, K>,
    offsets: &'a [V],
    data: &'a [u8],
}

impl<'a, K: Integer, V: IntoUsize> DictionaryDeserializer<'a, K, V> {
    pub fn new(
        keys_buffer: &'a [K],
        keys_validity: Option<BitBuffer<'a>>,
        data: &'a [u8],
        offsets: &'a [V],
    ) -> Self {
        Self {
            keys: ArrayBufferIterator::new(keys_buffer, keys_validity),
            offsets,
            data,
        }
    }

    pub fn next_str(&mut self) -> Result<&str> {
        let k: usize = self.keys.next_required()?.into_u64()?.try_into()?;
        let Some(start) = self.offsets.get(k) else {
            fail!("invalid index");
        };
        let start = start.into_usize()?;

        let Some(end) = self.offsets.get(k + 1) else {
            fail!("invalid index");
        };
        let end = end.into_usize()?;

        let s = std::str::from_utf8(&self.data[start..end])?;
        Ok(s)
    }
}

impl<'de, K: Integer, V: IntoUsize> SimpleDeserializer<'de> for DictionaryDeserializer<'de, K, V> {
    fn name() -> &'static str {
        "DictionaryDeserializer"
    }

    fn deserialize_any<VV: Visitor<'de>>(&mut self, visitor: VV) -> Result<VV::Value> {
        if self.keys.peek_next()? {
            self.deserialize_str(visitor)
        } else {
            self.keys.consume_next();
            visitor.visit_none()
        }
    }

    fn deserialize_option<VV: Visitor<'de>>(&mut self, visitor: VV) -> Result<VV::Value> {
        if self.keys.peek_next()? {
            visitor.visit_some(Mut(self))
        } else {
            self.keys.consume_next();
            visitor.visit_none()
        }
    }

    fn deserialize_str<VV: Visitor<'de>>(&mut self, visitor: VV) -> Result<VV::Value> {
        visitor.visit_str(self.next_str()?)
    }

    fn deserialize_string<VV: Visitor<'de>>(&mut self, visitor: VV) -> Result<VV::Value> {
        visitor.visit_string(self.next_str()?.to_owned())
    }
}
