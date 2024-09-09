use crate::internal::{
    arrow::BytesArrayView,
    error::{fail, Context, ContextSupport, Result},
    utils::{btree_map, Mut, NamedType, Offset},
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
            fail!("Tried to deserialize a value from an exhausted StringDeserializer");
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
            fail!("Tried to deserialize a value from StringDeserializer, but value is missing")
        };
        Ok(next)
    }

    pub fn peek_next(&self) -> Result<bool> {
        if self.next + 1 > self.view.offsets.len() {
            fail!("Tried to deserialize a value from an exhausted StringDeserializer");
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
    fn annotations(&self) -> std::collections::BTreeMap<String, String> {
        let data_type = match O::NAME {
            "i32" => "Utf8",
            "i64" => "LargeUtf8",
            _ => "<unknown>",
        };
        btree_map!("field" => self.path.clone(), "data_type" => data_type)
    }
}

impl<'a, O: NamedType + Offset> SimpleDeserializer<'a> for StringDeserializer<'a, O> {
    fn deserialize_any<V: serde::de::Visitor<'a>>(&mut self, visitor: V) -> Result<V::Value> {
        self.deserialize_any_impl(visitor).ctx(self)
    }

    fn deserialize_option<V: serde::de::Visitor<'a>>(&mut self, visitor: V) -> Result<V::Value> {
        self.deserialize_option_impl(visitor).ctx(self)
    }

    fn deserialize_str<V: serde::de::Visitor<'a>>(&mut self, visitor: V) -> Result<V::Value> {
        self.deserialize_str_impl(visitor).ctx(self)
    }

    fn deserialize_string<V: serde::de::Visitor<'a>>(&mut self, visitor: V) -> Result<V::Value> {
        self.deserialize_string_impl(visitor).ctx(self)
    }

    fn deserialize_enum<V: serde::de::Visitor<'a>>(
        &mut self,
        name: &'static str,
        variants: &'static [&'static str],
        visitor: V,
    ) -> Result<V::Value> {
        self.deserialize_enum_impl(name, variants, visitor)
            .ctx(self)
    }
}

impl<'a, O: NamedType + Offset> StringDeserializer<'a, O> {
    fn deserialize_any_impl<V: serde::de::Visitor<'a>>(&mut self, visitor: V) -> Result<V::Value> {
        if self.peek_next()? {
            self.deserialize_str(visitor)
        } else {
            self.consume_next();
            visitor.visit_none()
        }
    }

    fn deserialize_option_impl<V: serde::de::Visitor<'a>>(
        &mut self,
        visitor: V,
    ) -> Result<V::Value> {
        if self.peek_next()? {
            visitor.visit_some(Mut(self))
        } else {
            self.consume_next();
            visitor.visit_none()
        }
    }

    fn deserialize_str_impl<V: serde::de::Visitor<'a>>(&mut self, visitor: V) -> Result<V::Value> {
        visitor.visit_borrowed_str(self.next_required()?)
    }

    fn deserialize_string_impl<V: serde::de::Visitor<'a>>(
        &mut self,
        visitor: V,
    ) -> Result<V::Value> {
        visitor.visit_string(self.next_required()?.to_owned())
    }

    fn deserialize_enum_impl<V: serde::de::Visitor<'a>>(
        &mut self,
        _: &'static str,
        _: &'static [&'static str],
        visitor: V,
    ) -> Result<V::Value> {
        let variant = self.next_required()?;
        visitor.visit_enum(EnumAccess(variant))
    }
}
