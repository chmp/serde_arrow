use marrow::view::{BytesView, PrimitiveView};
use serde::de::Visitor;

use crate::internal::{
    error::{fail, set_default, try_, Context, ContextSupport, Result},
    utils::{array_view_ext::ViewAccess, Mut, Offset},
};

use super::{
    enums_as_string_impl::EnumAccess, integer_deserializer::Integer,
    random_access_deserializer::RandomAccessDeserializer, simple_deserializer::SimpleDeserializer,
    utils::ArrayBufferIterator,
};

pub struct DictionaryDeserializer<'a, K: Integer, V: Offset> {
    path: String,
    keys: PrimitiveView<'a, K>,
    values: BytesView<'a, V>,
    keys_iterator: ArrayBufferIterator<'a, K>,
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
            keys: keys.clone(),
            values: values.clone(),
            keys_iterator: ArrayBufferIterator::new(keys.values, keys.validity),
            offsets: values.offsets,
            data: values.data,
        })
    }

    pub fn next_str(&mut self) -> Result<&str> {
        let k: usize = self.keys_iterator.next_required()?.into_u64()?.try_into()?;
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

    pub fn get_str(&self, idx: usize) -> Result<&str> {
        let key: usize = self.keys.get_required(idx)?.into_i64()?.try_into()?;
        let value: &str = self.values.get_required(key)?;
        Ok(value)
    }
}

impl<K: Integer, V: Offset> Context for DictionaryDeserializer<'_, K, V> {
    fn annotate(&self, annotations: &mut std::collections::BTreeMap<String, String>) {
        set_default(annotations, "field", &self.path);
        set_default(annotations, "data_type", "Dictionary(..)");
    }
}

impl<'de, K: Integer, V: Offset> SimpleDeserializer<'de> for DictionaryDeserializer<'de, K, V> {
    fn deserialize_any<VV: Visitor<'de>>(&mut self, visitor: VV) -> Result<VV::Value> {
        try_(|| {
            if self.keys_iterator.peek_next()? {
                self.deserialize_str(visitor)
            } else {
                self.keys_iterator.consume_next();
                visitor.visit_none()
            }
        })
        .ctx(self)
    }

    fn deserialize_option<VV: Visitor<'de>>(&mut self, visitor: VV) -> Result<VV::Value> {
        try_(|| {
            if self.keys_iterator.peek_next()? {
                visitor.visit_some(Mut(self))
            } else {
                self.keys_iterator.consume_next();
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

impl<'de, K: Integer, V: Offset> RandomAccessDeserializer<'de>
    for DictionaryDeserializer<'de, K, V>
{
    fn is_some(&self, idx: usize) -> Result<bool> {
        self.keys.is_some(idx)
    }

    fn deserialize_any_some<VV: Visitor<'de>>(&self, visitor: VV, idx: usize) -> Result<VV::Value> {
        self.deserialize_str(visitor, idx)
    }

    fn deserialize_str<VV: Visitor<'de>>(&self, visitor: VV, idx: usize) -> Result<VV::Value> {
        try_(|| visitor.visit_str(self.get_str(idx)?)).ctx(self)
    }

    fn deserialize_string<VV: Visitor<'de>>(&self, visitor: VV, idx: usize) -> Result<VV::Value> {
        try_(|| visitor.visit_string(self.get_str(idx)?.to_owned())).ctx(self)
    }

    fn deserialize_enum<VV: Visitor<'de>>(
        &self,
        _: &'static str,
        _: &'static [&'static str],
        visitor: VV,
        idx: usize,
    ) -> Result<VV::Value> {
        try_(|| visitor.visit_enum(EnumAccess(self.get_str(idx)?))).ctx(self)
    }
}
