use serde::de::Visitor;

use crate::internal::{
    common::{BitBuffer, Mut},
    error::Result,
};

use super::{simple_deserializer::SimpleDeserializer, utils::ArrayBufferIterator};

pub trait Integer: Sized + Copy {
    fn deserialize_any<'de, S: SimpleDeserializer<'de>, V: Visitor<'de>>(
        deser: &mut S,
        visitor: V,
    ) -> Result<V::Value>;

    fn into_bool(self) -> Result<bool>;

    fn into_i8(self) -> Result<i8>;
    fn into_i16(self) -> Result<i16>;
    fn into_i32(self) -> Result<i32>;
    fn into_i64(self) -> Result<i64>;

    fn into_u8(self) -> Result<u8>;
    fn into_u16(self) -> Result<u16>;
    fn into_u32(self) -> Result<u32>;
    fn into_u64(self) -> Result<u64>;
}

pub struct IntegerDeserializer<'a, T: Integer>(ArrayBufferIterator<'a, T>);

impl<'a, T: Integer> IntegerDeserializer<'a, T> {
    pub fn new(buffer: &'a [T], validity: Option<BitBuffer<'a>>) -> Self {
        Self(ArrayBufferIterator::new(buffer, validity))
    }
}

impl<'de, T: Integer> SimpleDeserializer<'de> for IntegerDeserializer<'de, T> {
    fn name() -> &'static str {
        "IntegerDeserializer"
    }

    fn deserialize_any<V: Visitor<'de>>(&mut self, visitor: V) -> Result<V::Value> {
        if self.0.peek_next()? {
            T::deserialize_any(self, visitor)
        } else {
            self.0.consume_next();
            visitor.visit_none()
        }
    }

    fn deserialize_option<V: Visitor<'de>>(&mut self, visitor: V) -> Result<V::Value> {
        if self.0.peek_next()? {
            visitor.visit_some(Mut(self))
        } else {
            self.0.consume_next();
            visitor.visit_none()
        }
    }

    fn deserialize_bool<V: Visitor<'de>>(&mut self, visitor: V) -> Result<V::Value> {
        visitor.visit_bool(self.0.next_required()?.into_bool()?)
    }

    fn deserialize_char<V: Visitor<'de>>(&mut self, visitor: V) -> Result<V::Value> {
        visitor.visit_char(self.0.next_required()?.into_u32()?.try_into()?)
    }

    fn deserialize_u8<V: Visitor<'de>>(&mut self, visitor: V) -> Result<V::Value> {
        visitor.visit_u8(self.0.next_required()?.into_u8()?)
    }

    fn deserialize_u16<V: Visitor<'de>>(&mut self, visitor: V) -> Result<V::Value> {
        visitor.visit_u16(self.0.next_required()?.into_u16()?)
    }

    fn deserialize_u32<V: Visitor<'de>>(&mut self, visitor: V) -> Result<V::Value> {
        visitor.visit_u32(self.0.next_required()?.into_u32()?)
    }

    fn deserialize_u64<V: Visitor<'de>>(&mut self, visitor: V) -> Result<V::Value> {
        visitor.visit_u64(self.0.next_required()?.into_u64()?)
    }

    fn deserialize_i8<V: Visitor<'de>>(&mut self, visitor: V) -> Result<V::Value> {
        visitor.visit_i8(self.0.next_required()?.into_i8()?)
    }

    fn deserialize_i16<V: Visitor<'de>>(&mut self, visitor: V) -> Result<V::Value> {
        visitor.visit_i16(self.0.next_required()?.into_i16()?)
    }

    fn deserialize_i32<V: Visitor<'de>>(&mut self, visitor: V) -> Result<V::Value> {
        visitor.visit_i32(self.0.next_required()?.into_i32()?)
    }

    fn deserialize_i64<V: Visitor<'de>>(&mut self, visitor: V) -> Result<V::Value> {
        visitor.visit_i64(self.0.next_required()?.into_i64()?)
    }
}
