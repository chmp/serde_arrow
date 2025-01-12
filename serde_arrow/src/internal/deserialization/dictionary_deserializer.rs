use marrow::view::{BytesView, PrimitiveView};
use serde::de::Visitor;

use crate::internal::{
    error::{fail, set_default, try_, Context, ContextSupport, Result},
    utils::{Mut, Offset},
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
    pub fn new(path: String, keys: PrimitiveView<'a, K>, values: BytesView<'a, V>) -> Result<Self> {
        if values.validity.is_some() {
            // TODO: check whether all values are defined?
            fail!("Null for non-nullable type: dictionaries do not support nullable values");
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
            fail!("Invalid index");
        };
        let start = start.try_into_usize()?;

        let Some(end) = self.offsets.get(k + 1) else {
            fail!("Invalid index");
        };
        let end = end.try_into_usize()?;

        let s = std::str::from_utf8(&self.data[start..end])?;
        Ok(s)
    }
}

impl<'de, K: Integer, V: Offset> Context for DictionaryDeserializer<'de, K, V> {
    fn annotate(&self, annotations: &mut std::collections::BTreeMap<String, String>) {
        set_default(annotations, "field", &self.path);
        set_default(annotations, "data_type", "Dictionary(..)");
    }
}

impl<'de, K: Integer, V: Offset> SimpleDeserializer<'de> for DictionaryDeserializer<'de, K, V> {
    fn deserialize_any<VV: Visitor<'de>>(&mut self, visitor: VV) -> Result<VV::Value> {
        try_(|| {
            if self.keys.peek_next()? {
                self.deserialize_str(visitor)
            } else {
                self.keys.consume_next();
                visitor.visit_none()
            }
        })
        .ctx(self)
    }

    fn deserialize_option<VV: Visitor<'de>>(&mut self, visitor: VV) -> Result<VV::Value> {
        try_(|| {
            if self.keys.peek_next()? {
                visitor.visit_some(Mut(self))
            } else {
                self.keys.consume_next();
                visitor.visit_none()
            }
        })
        .ctx(self)
    }

    fn deserialize_str<VV: Visitor<'de>>(&mut self, visitor: VV) -> Result<VV::Value> {
        try_(|| visitor.visit_str(self.next_str()?)).ctx(self)
    }

    fn deserialize_string<VV: Visitor<'de>>(&mut self, visitor: VV) -> Result<VV::Value> {
        try_(|| visitor.visit_string(self.next_str()?.to_owned())).ctx(self)
    }

    fn deserialize_enum<VV: Visitor<'de>>(
        &mut self,
        _: &'static str,
        _: &'static [&'static str],
        visitor: VV,
    ) -> Result<VV::Value> {
        try_(|| {
            let variant = self.next_str()?;
            visitor.visit_enum(EnumAccess(variant))
        })
        .ctx(self)
    }
}
