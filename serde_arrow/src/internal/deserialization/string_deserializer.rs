use marrow::view::{BytesView, BytesViewView};

use crate::internal::{
    error::{fail, set_default, try_, Context, ContextSupport, Result},
    utils::{Mut, Offset},
};

use super::{
    enums_as_string_impl::EnumAccess, simple_deserializer::SimpleDeserializer, utils::bitset_is_set,
};

pub trait BytesAccess<'a> {
    fn get_bytes(&self, idx: usize) -> Result<Option<&'a [u8]>>;
}

impl<'a, O: Offset> BytesAccess<'a> for BytesView<'a, O> {
    fn get_bytes(&self, idx: usize) -> Result<Option<&'a [u8]>> {
        if idx + 1 > self.offsets.len() {
            fail!("Exhausted deserializer: tried to deserialize a value from an exhausted StringDeserializer");
        }

        if let Some(validity) = &self.validity {
            if !bitset_is_set(validity, idx)? {
                return Ok(None);
            }
        }

        let start = self.offsets[idx].try_into_usize()?;
        let end = self.offsets[idx + 1].try_into_usize()?;
        Ok(Some(&self.data[start..end]))
    }
}

impl<'a> BytesAccess<'a> for BytesViewView<'a> {
    fn get_bytes(&self, idx: usize) -> Result<Option<&'a [u8]>> {
        let Some(desc) = self.data.get(idx) else {
            fail!("Exhausted deserializer: tried to deserialize a value from an exhausted StringDeserializer");
        };

        if let Some(validity) = &self.validity {
            if !bitset_is_set(validity, idx)? {
                return Ok(None);
            }
        }

        let len = (*desc as u32) as usize;
        let res = || -> Option<&'a [u8]> {
            if len <= 12 {
                let bytes: &[u8] = bytemuck::try_cast_slice(std::slice::from_ref(desc)).ok()?;
                bytes.get(4..4 + len)
            } else {
                let buf_idx = ((*desc >> 64) as u32) as usize;
                let offset = ((*desc >> 96) as u32) as usize;
                self.buffers.get(buf_idx)?.get(offset..offset + len)
            }
        }();

        if res.is_none() {
            fail!("invalid state in bytes deserialization");
        }
        Ok(res)
    }
}

pub trait StringDeserializerDataType {
    const DATA_TYPE_NAME: &'static str;
}

impl StringDeserializerDataType for BytesView<'_, i32> {
    const DATA_TYPE_NAME: &'static str = "Utf8";
}

impl StringDeserializerDataType for BytesView<'_, i64> {
    const DATA_TYPE_NAME: &'static str = "LargeUtf8";
}

impl StringDeserializerDataType for BytesViewView<'_> {
    const DATA_TYPE_NAME: &'static str = "Utf8View";
}

pub struct StringDeserializer<V> {
    pub path: String,
    pub view: V,
    pub next: usize,
}

impl<'a, V: BytesAccess<'a>> StringDeserializer<V> {
    pub fn new(path: String, view: V) -> Self {
        Self {
            path,
            view,
            next: 0,
        }
    }

    pub fn next(&mut self) -> Result<Option<&'a str>> {
        let res = if let Some(data) = self.view.get_bytes(self.next)? {
            Some(std::str::from_utf8(data)?)
        } else {
            None
        };
        self.next += 1;
        Ok(res)
    }

    pub fn next_required(&mut self) -> Result<&'a str> {
        let Some(next) = self.next()? else {
            fail!("Exhausted deserializer: tried to deserialize a value from StringDeserializer, but value is missing")
        };
        Ok(next)
    }

    pub fn peek_next(&self) -> Result<bool> {
        Ok(self.view.get_bytes(self.next)?.is_some())
    }

    pub fn consume_next(&mut self) {
        self.next += 1;
    }
}

impl<V: StringDeserializerDataType> Context for StringDeserializer<V> {
    fn annotate(&self, annotations: &mut std::collections::BTreeMap<String, String>) {
        set_default(annotations, "field", &self.path);
        set_default(annotations, "data_type", V::DATA_TYPE_NAME);
    }
}

impl<'a, VV: BytesAccess<'a> + StringDeserializerDataType> SimpleDeserializer<'a>
    for StringDeserializer<VV>
{
    fn deserialize_any<V: serde::de::Visitor<'a>>(&mut self, visitor: V) -> Result<V::Value> {
        try_(|| {
            if self.peek_next()? {
                self.deserialize_str(visitor)
            } else {
                self.consume_next();
                visitor.visit_none()
            }
        })
        .ctx(self)
    }

    fn deserialize_option<V: serde::de::Visitor<'a>>(&mut self, visitor: V) -> Result<V::Value> {
        try_(|| {
            if self.peek_next()? {
                visitor.visit_some(Mut(self))
            } else {
                self.consume_next();
                visitor.visit_none()
            }
        })
        .ctx(self)
    }

    fn deserialize_str<V: serde::de::Visitor<'a>>(&mut self, visitor: V) -> Result<V::Value> {
        try_(|| visitor.visit_borrowed_str(self.next_required()?)).ctx(self)
    }

    fn deserialize_string<V: serde::de::Visitor<'a>>(&mut self, visitor: V) -> Result<V::Value> {
        try_(|| visitor.visit_string(self.next_required()?.to_owned())).ctx(self)
    }

    fn deserialize_bytes<V: serde::de::Visitor<'a>>(&mut self, visitor: V) -> Result<V::Value> {
        try_(|| visitor.visit_bytes(self.next_required()?.as_bytes())).ctx(self)
    }

    fn deserialize_byte_buf<V: serde::de::Visitor<'a>>(&mut self, visitor: V) -> Result<V::Value> {
        try_(|| visitor.visit_byte_buf(self.next_required()?.to_owned().into_bytes())).ctx(self)
    }

    fn deserialize_enum<V: serde::de::Visitor<'a>>(
        &mut self,
        _name: &'static str,
        _variants: &'static [&'static str],
        visitor: V,
    ) -> Result<V::Value> {
        try_(|| {
            let variant = self.next_required()?;
            visitor.visit_enum(EnumAccess(variant))
        })
        .ctx(self)
    }
}
