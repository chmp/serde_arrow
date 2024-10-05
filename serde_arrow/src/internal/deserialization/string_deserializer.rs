use crate::internal::{
    arrow::BytesArrayView,
    error::{fail, set_default, try_, Context, ContextSupport, Result},
    utils::{Mut, NamedType, Offset},
};

use super::{
    enums_as_string_impl::EnumAccess, simple_deserializer::SimpleDeserializer, utils::bitset_is_set,
};

pub struct StringDeserializer<'a, O: Offset> {
    pub path: String,
    pub view: BytesArrayView<'a, O>,
    pub next: usize,
}

impl<'a, O: Offset> StringDeserializer<'a, O> {
    pub fn new(path: String, view: BytesArrayView<'a, O>) -> Self {
        Self {
            path,
            view,
            next: 0,
        }
    }

    pub fn next(&mut self) -> Result<Option<&'a str>> {
        if self.next + 1 > self.view.offsets.len() {
            fail!("Exhausted deserializer: tried to deserialize a value from an exhausted StringDeserializer");
        }

        if let Some(validity) = &self.view.validity {
            if !bitset_is_set(validity, self.next)? {
                return Ok(None);
            }
        }

        let start = self.view.offsets[self.next].try_into_usize()?;
        let end = self.view.offsets[self.next + 1].try_into_usize()?;
        let s = std::str::from_utf8(&self.view.data[start..end])?;

        self.next += 1;

        Ok(Some(s))
    }

    pub fn next_required(&mut self) -> Result<&'a str> {
        let Some(next) = self.next()? else {
            fail!("Exhausted deserializer: tried to deserialize a value from StringDeserializer, but value is missing")
        };
        Ok(next)
    }

    pub fn peek_next(&self) -> Result<bool> {
        if self.next + 1 > self.view.offsets.len() {
            fail!("Exhausted deserializer: tried to deserialize a value from an exhausted StringDeserializer");
        }

        if let Some(validity) = &self.view.validity {
            if !bitset_is_set(validity, self.next)? {
                return Ok(false);
            }
        }
        Ok(true)
    }

    pub fn consume_next(&mut self) {
        self.next += 1;
    }
}

impl<'a, O: NamedType + Offset> Context for StringDeserializer<'a, O> {
    fn annotate(&self, annotations: &mut std::collections::BTreeMap<String, String>) {
        set_default(annotations, "field", &self.path);
        set_default(
            annotations,
            "data_type",
            match O::NAME {
                "i32" => "Utf8",
                "i64" => "LargeUtf8",
                _ => "<unknown>",
            },
        );
    }
}

impl<'a, O: NamedType + Offset> SimpleDeserializer<'a> for StringDeserializer<'a, O> {
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
