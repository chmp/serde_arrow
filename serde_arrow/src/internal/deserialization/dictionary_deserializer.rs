use serde::de::Visitor;

use crate::internal::{
    arrow::{BytesArrayView, PrimitiveArrayView},
    error::{fail, Context, Result},
    utils::{btree_map, Mut, Offset},
};

use super::{
    enums_as_string_impl::EnumAccess, integer_deserializer::Integer,
    simple_deserializer::SimpleDeserializer, utils::ArrayBufferIterator,
};

pub struct DictionaryDeserializer<'a, K: Integer, V: Offset> {
    path: String,
    keys: ArrayBufferIterator<'a, K>,
    offsets: &'a [V],
    data: &'a [u8],
}

impl<'a, K: Integer, V: Offset> DictionaryDeserializer<'a, K, V> {
    pub fn new(
        path: String,
        keys: PrimitiveArrayView<'a, K>,
        values: BytesArrayView<'a, V>,
    ) -> Result<Self> {
        if values.validity.is_some() {
            // TODO: check whether all values are defined?
            fail!("dictionaries with nullable values are not supported");
        }
        Ok(Self {
            path,
            keys: ArrayBufferIterator::new(keys.values, keys.validity),
            offsets: values.offsets,
            data: values.data,
        })
    }

    pub fn next_str(&mut self) -> Result<&str> {
        let k: usize = self.keys.next_required()?.into_u64()?.try_into()?;
        let Some(start) = self.offsets.get(k) else {
            fail!("invalid index");
        };
        let start = start.try_into_usize()?;

        let Some(end) = self.offsets.get(k + 1) else {
            fail!("invalid index");
        };
        let end = end.try_into_usize()?;

        let s = std::str::from_utf8(&self.data[start..end])?;
        Ok(s)
    }
}

impl<'de, K: Integer, V: Offset> Context for DictionaryDeserializer<'de, K, V> {
    fn annotations(&self) -> std::collections::BTreeMap<String, String> {
        btree_map!("path" => self.path.clone(), "data_type" => "Dictionary(..)")
    }
}

impl<'de, K: Integer, V: Offset> SimpleDeserializer<'de> for DictionaryDeserializer<'de, K, V> {
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

    fn deserialize_enum<VV: Visitor<'de>>(
        &mut self,
        _: &'static str,
        _: &'static [&'static str],
        visitor: VV,
    ) -> Result<VV::Value> {
        let variant = self.next_str()?;
        visitor.visit_enum(EnumAccess(variant))
    }
}
