use serde::de::Visitor;

use crate::{internal::error::fail, Result};

use super::simple_deserializer::SimpleDeserializer;

pub trait Primitive {
    fn deserialize_any<'de, S: SimpleDeserializer<'de>, V: Visitor<'de>>(deser: &mut S, visitor: V) -> Result<V::Value>;

    fn into_i8(&self) -> Result<i8>;
    fn into_i16(&self) -> Result<i16>;
    fn into_i32(&self) -> Result<i32>;
    fn into_i64(&self) -> Result<i64>;
}

pub struct PrimitiveDeserializer<'a, T: Primitive> {
    pub buffer: &'a [T],
    pub next: usize,
}

impl<'a, T: Primitive> PrimitiveDeserializer<'a, T> {
    pub fn new(buffer: &'a [T]) -> Self {
        Self {
            buffer,
            next: 0,
        }
    }

    fn next(&mut self) -> Result<&T> {
        if self.next < self.buffer.len() {
            let val = &self.buffer[self.next];
            self.next += 1;
            Ok(val)
        } else {
            fail!("Exhausted PrimitiveDeserializer");
        }
    }
}

impl Primitive for i8 {
    fn deserialize_any<'de, S: SimpleDeserializer<'de>, V: Visitor<'de>>(deser: &mut S, visitor: V) -> Result<V::Value> {
        deser.deserialize_i32(visitor)
    }

    fn into_i8(&self) -> Result<i8> {
        Ok(*self)
    }

    fn into_i16(&self) -> Result<i16> {
        Ok(*self as i16)
    }

    fn into_i32(&self) -> Result<i32> {
        Ok(*self as i32)
    }

    fn into_i64(&self) -> Result<i64> {
        Ok(*self as i64)
    }
}

impl Primitive for i16 {
    fn deserialize_any<'de, S: SimpleDeserializer<'de>, V: Visitor<'de>>(deser: &mut S, visitor: V) -> Result<V::Value> {
        deser.deserialize_i16(visitor)
    }

    fn into_i8(&self) -> Result<i8> {
        Ok((*self).try_into()?)
    }

    fn into_i16(&self) -> Result<i16> {
        Ok(*self)
    }

    fn into_i32(&self) -> Result<i32> {
        Ok(*self as i32)
    }

    fn into_i64(&self) -> Result<i64> {
        Ok(*self as i64)
    }
}

impl Primitive for i32 {
    fn deserialize_any<'de, S: SimpleDeserializer<'de>, V: Visitor<'de>>(deser: &mut S, visitor: V) -> Result<V::Value> {
        deser.deserialize_i32(visitor)
    }

    fn into_i8(&self) -> Result<i8> {
        Ok((*self).try_into()?)
    }

    fn into_i16(&self) -> Result<i16> {
        Ok((*self).try_into()?)
    }

    fn into_i32(&self) -> Result<i32> {
        Ok(*self)
    }

    fn into_i64(&self) -> Result<i64> {
        Ok(*self as i64)
    }
}

impl Primitive for i64 {
    fn deserialize_any<'de, S: SimpleDeserializer<'de>, V: Visitor<'de>>(deser: &mut S, visitor: V) -> Result<V::Value> {
        deser.deserialize_i64(visitor)
    }

    fn into_i8(&self) -> Result<i8> {
        Ok((*self).try_into()?)
    }

    fn into_i16(&self) -> Result<i16> {
        Ok((*self).try_into()?)
    }

    fn into_i32(&self) -> Result<i32> {
        Ok((*self).try_into()?)
    }

    fn into_i64(&self) -> Result<i64> {
        Ok(*self)
    }
}

impl<'de, T: Primitive> SimpleDeserializer<'de> for PrimitiveDeserializer<'de, T> {
    fn name() -> &'static str {
        "PrimitiveDeserializer"
    }

    fn deserialize_any<V: serde::de::Visitor<'de>>(&mut self, visitor: V) -> Result<V::Value> {
        T::deserialize_any(self, visitor)
    }
    
    fn deserialize_i8<V: serde::de::Visitor<'de>>(&mut self, visitor: V) -> Result<V::Value> {
        visitor.visit_i8(self.next()?.into_i8()?)
    }

    fn deserialize_i16<V: serde::de::Visitor<'de>>(&mut self, visitor: V) -> Result<V::Value> {
        visitor.visit_i16(self.next()?.into_i16()?)
    }

    fn deserialize_i32<V: serde::de::Visitor<'de>>(&mut self, visitor: V) -> Result<V::Value> {
        visitor.visit_i32(self.next()?.into_i32()?)
    }

    fn deserialize_i64<V: serde::de::Visitor<'de>>(&mut self, visitor: V) -> Result<V::Value> {
        visitor.visit_i64(self.next()?.into_i64()?)
    }
}