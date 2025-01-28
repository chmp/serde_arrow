use marrow::view::{BytesView, BytesViewView};

use crate::internal::{
    error::{fail, set_default, try_, Context, ContextSupport, Result},
    utils::Mut,
};

use super::{
    enums_as_string_impl::EnumAccess, simple_deserializer::SimpleDeserializer, utils::BytesAccess,
};

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
