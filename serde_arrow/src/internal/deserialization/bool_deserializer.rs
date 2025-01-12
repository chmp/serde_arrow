use marrow::view::BooleanView;
use serde::de::Visitor;

use crate::internal::{
    error::{fail, set_default, try_, Context, ContextSupport, Error, Result},
    utils::Mut,
};

use super::{simple_deserializer::SimpleDeserializer, utils::bitset_is_set};

pub struct BoolDeserializer<'a> {
    pub path: String,
    pub view: BooleanView<'a>,
    pub next: usize,
}

impl<'a> BoolDeserializer<'a> {
    pub fn new(path: String, view: BooleanView<'a>) -> Self {
        Self {
            path,
            view,
            next: 0,
        }
    }

    fn next(&mut self) -> Result<Option<bool>> {
        if self.next >= self.view.len {
            fail!("Exhausted deserializer");
        }
        if let Some(validty) = &self.view.validity {
            if !bitset_is_set(validty, self.next)? {
                self.next += 1;
                return Ok(None);
            }
        }

        let val = bitset_is_set(&self.view.values, self.next)?;
        self.next += 1;
        Ok(Some(val))
    }

    fn next_required(&mut self) -> Result<bool> {
        if let Some(val) = self.next()? {
            Ok(val)
        } else {
            fail!("Missing value");
        }
    }

    fn peek_next(&self) -> Result<bool> {
        if self.next >= self.view.len {
            fail!("Exhausted deserializer");
        } else if let Some(validity) = &self.view.validity {
            bitset_is_set(validity, self.next)
        } else {
            Ok(true)
        }
    }

    fn consume_next(&mut self) {
        self.next += 1;
    }
}

impl<'de> Context for BoolDeserializer<'de> {
    fn annotate(&self, annotations: &mut std::collections::BTreeMap<String, String>) {
        set_default(annotations, "field", &self.path);
        set_default(annotations, "data_type", "Boolean");
    }
}

impl<'de> SimpleDeserializer<'de> for BoolDeserializer<'de> {
    fn deserialize_any<V: Visitor<'de>>(&mut self, visitor: V) -> Result<V::Value> {
        try_(|| {
            if self.peek_next()? {
                self.deserialize_bool(visitor)
            } else {
                self.consume_next();
                visitor.visit_none::<Error>()
            }
        })
        .ctx(self)
    }

    fn deserialize_option<V: Visitor<'de>>(&mut self, visitor: V) -> Result<V::Value> {
        try_(|| {
            if self.peek_next()? {
                visitor.visit_some(Mut(self))
            } else {
                self.consume_next();
                visitor.visit_none::<Error>()
            }
        })
        .ctx(self)
    }

    fn deserialize_bool<V: Visitor<'de>>(&mut self, visitor: V) -> Result<V::Value> {
        try_(|| visitor.visit_bool::<Error>(self.next_required()?)).ctx(self)
    }

    fn deserialize_u8<V: Visitor<'de>>(&mut self, visitor: V) -> Result<V::Value> {
        try_(|| visitor.visit_u8::<Error>(if self.next_required()? { 1 } else { 0 })).ctx(self)
    }

    fn deserialize_u16<V: Visitor<'de>>(&mut self, visitor: V) -> Result<V::Value> {
        try_(|| visitor.visit_u16::<Error>(if self.next_required()? { 1 } else { 0 })).ctx(self)
    }

    fn deserialize_u32<V: Visitor<'de>>(&mut self, visitor: V) -> Result<V::Value> {
        try_(|| visitor.visit_u32::<Error>(if self.next_required()? { 1 } else { 0 })).ctx(self)
    }

    fn deserialize_u64<V: Visitor<'de>>(&mut self, visitor: V) -> Result<V::Value> {
        try_(|| visitor.visit_u64::<Error>(if self.next_required()? { 1 } else { 0 })).ctx(self)
    }

    fn deserialize_i8<V: Visitor<'de>>(&mut self, visitor: V) -> Result<V::Value> {
        try_(|| visitor.visit_i8::<Error>(if self.next_required()? { 1 } else { 0 })).ctx(self)
    }

    fn deserialize_i16<V: Visitor<'de>>(&mut self, visitor: V) -> Result<V::Value> {
        try_(|| visitor.visit_i16::<Error>(if self.next_required()? { 1 } else { 0 })).ctx(self)
    }

    fn deserialize_i32<V: Visitor<'de>>(&mut self, visitor: V) -> Result<V::Value> {
        try_(|| visitor.visit_i32::<Error>(if self.next_required()? { 1 } else { 0 })).ctx(self)
    }

    fn deserialize_i64<V: Visitor<'de>>(&mut self, visitor: V) -> Result<V::Value> {
        try_(|| visitor.visit_i64::<Error>(if self.next_required()? { 1 } else { 0 })).ctx(self)
    }
}
