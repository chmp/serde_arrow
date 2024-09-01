use serde::de::Visitor;

use crate::internal::{
    arrow::BooleanArrayView,
    error::{fail, Context, Result},
    utils::{btree_map, Mut},
};

use super::{simple_deserializer::SimpleDeserializer, utils::bitset_is_set};

pub struct BoolDeserializer<'a> {
    pub path: String,
    pub view: BooleanArrayView<'a>,
    pub next: usize,
}

impl<'a> BoolDeserializer<'a> {
    pub fn new(path: String, view: BooleanArrayView<'a>) -> Self {
        Self {
            path,
            view,
            next: 0,
        }
    }

    fn next(&mut self) -> Result<Option<bool>> {
        if self.next >= self.view.len {
            fail!("Exhausted BoolDeserializer");
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
            fail!("Exhausted BoolDeserializer");
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
    fn annotations(&self) -> std::collections::BTreeMap<String, String> {
        btree_map!("path" => self.path.clone(), "data_type" => "Boolean")
    }
}

impl<'de> SimpleDeserializer<'de> for BoolDeserializer<'de> {
    fn deserialize_any<V: Visitor<'de>>(&mut self, visitor: V) -> Result<V::Value> {
        if self.peek_next()? {
            self.deserialize_bool(visitor)
        } else {
            self.consume_next();
            visitor.visit_none()
        }
    }

    fn deserialize_option<V: Visitor<'de>>(&mut self, visitor: V) -> Result<V::Value> {
        if self.peek_next()? {
            visitor.visit_some(Mut(self))
        } else {
            self.consume_next();
            visitor.visit_none()
        }
    }

    fn deserialize_bool<V: Visitor<'de>>(&mut self, visitor: V) -> Result<V::Value> {
        visitor.visit_bool(self.next_required()?)
    }

    fn deserialize_u8<V: Visitor<'de>>(&mut self, visitor: V) -> Result<V::Value> {
        visitor.visit_u8(if self.next_required()? { 1 } else { 0 })
    }

    fn deserialize_u16<V: Visitor<'de>>(&mut self, visitor: V) -> Result<V::Value> {
        visitor.visit_u16(if self.next_required()? { 1 } else { 0 })
    }

    fn deserialize_u32<V: Visitor<'de>>(&mut self, visitor: V) -> Result<V::Value> {
        visitor.visit_u32(if self.next_required()? { 1 } else { 0 })
    }

    fn deserialize_u64<V: Visitor<'de>>(&mut self, visitor: V) -> Result<V::Value> {
        visitor.visit_u64(if self.next_required()? { 1 } else { 0 })
    }

    fn deserialize_i8<V: Visitor<'de>>(&mut self, visitor: V) -> Result<V::Value> {
        visitor.visit_i8(if self.next_required()? { 1 } else { 0 })
    }

    fn deserialize_i16<V: Visitor<'de>>(&mut self, visitor: V) -> Result<V::Value> {
        visitor.visit_i16(if self.next_required()? { 1 } else { 0 })
    }

    fn deserialize_i32<V: Visitor<'de>>(&mut self, visitor: V) -> Result<V::Value> {
        visitor.visit_i32(if self.next_required()? { 1 } else { 0 })
    }

    fn deserialize_i64<V: Visitor<'de>>(&mut self, visitor: V) -> Result<V::Value> {
        visitor.visit_i64(if self.next_required()? { 1 } else { 0 })
    }
}
