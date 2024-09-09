use serde::de::Visitor;

use crate::internal::{
    arrow::PrimitiveArrayView,
    error::{Context, ContextSupport, Result},
    utils::{btree_map, Mut, NamedType},
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

pub struct IntegerDeserializer<'a, T: Integer> {
    path: String,
    array: ArrayBufferIterator<'a, T>,
}

impl<'a, T: Integer> IntegerDeserializer<'a, T> {
    pub fn new(path: String, view: PrimitiveArrayView<'a, T>) -> Self {
        Self {
            path,
            array: ArrayBufferIterator::new(view.values, view.validity),
        }
    }
}

impl<'de, T: NamedType + Integer> Context for IntegerDeserializer<'de, T> {
    fn annotations(&self) -> std::collections::BTreeMap<String, String> {
        let data_type = match T::NAME {
            "i8" => "Int8",
            "i16" => "Int16",
            "i32" => "Int32",
            "i64" => "Int64",
            "u8" => "UInt8",
            "u16" => "UInt16",
            "u32" => "UInt32",
            "u64" => "UInt64",
            _ => "<unknown>",
        };
        btree_map!("field" => self.path.clone(), "data_type" => data_type)
    }
}

impl<'de, T: NamedType + Integer> SimpleDeserializer<'de> for IntegerDeserializer<'de, T> {
    fn deserialize_any<V: Visitor<'de>>(&mut self, visitor: V) -> Result<V::Value> {
        self.deserialize_any_impl(visitor).ctx(self)
    }

    fn deserialize_option<V: Visitor<'de>>(&mut self, visitor: V) -> Result<V::Value> {
        self.deserialize_option_impl(visitor).ctx(self)
    }

    fn deserialize_bool<V: Visitor<'de>>(&mut self, visitor: V) -> Result<V::Value> {
        self.deserialize_bool_impl(visitor).ctx(self)
    }

    fn deserialize_char<V: Visitor<'de>>(&mut self, visitor: V) -> Result<V::Value> {
        self.deserialize_char_impl(visitor).ctx(self)
    }

    fn deserialize_u8<V: Visitor<'de>>(&mut self, visitor: V) -> Result<V::Value> {
        self.deserialize_u8_impl(visitor).ctx(self)
    }

    fn deserialize_u16<V: Visitor<'de>>(&mut self, visitor: V) -> Result<V::Value> {
        self.deserialize_u16_impl(visitor).ctx(self)
    }

    fn deserialize_u32<V: Visitor<'de>>(&mut self, visitor: V) -> Result<V::Value> {
        self.deserialize_u32_impl(visitor).ctx(self)
    }

    fn deserialize_u64<V: Visitor<'de>>(&mut self, visitor: V) -> Result<V::Value> {
        self.deserialize_u64_impl(visitor).ctx(self)
    }

    fn deserialize_i8<V: Visitor<'de>>(&mut self, visitor: V) -> Result<V::Value> {
        self.deserialize_i8_impl(visitor).ctx(self)
    }

    fn deserialize_i16<V: Visitor<'de>>(&mut self, visitor: V) -> Result<V::Value> {
        self.deserialize_i16_impl(visitor).ctx(self)
    }

    fn deserialize_i32<V: Visitor<'de>>(&mut self, visitor: V) -> Result<V::Value> {
        self.deserialize_i32_impl(visitor).ctx(self)
    }

    fn deserialize_i64<V: Visitor<'de>>(&mut self, visitor: V) -> Result<V::Value> {
        self.deserialize_i64_impl(visitor).ctx(self)
    }
}

impl<'de, T: NamedType + Integer> IntegerDeserializer<'de, T> {
    fn deserialize_any_impl<V: Visitor<'de>>(&mut self, visitor: V) -> Result<V::Value> {
        if self.array.peek_next()? {
            T::deserialize_any(self, visitor)
        } else {
            self.array.consume_next();
            visitor.visit_none()
        }
    }

    fn deserialize_option_impl<V: Visitor<'de>>(&mut self, visitor: V) -> Result<V::Value> {
        if self.array.peek_next()? {
            visitor.visit_some(Mut(self))
        } else {
            self.array.consume_next();
            visitor.visit_none()
        }
    }

    fn deserialize_bool_impl<V: Visitor<'de>>(&mut self, visitor: V) -> Result<V::Value> {
        visitor.visit_bool(self.array.next_required()?.into_bool()?)
    }

    fn deserialize_char_impl<V: Visitor<'de>>(&mut self, visitor: V) -> Result<V::Value> {
        visitor.visit_char(self.array.next_required()?.into_u32()?.try_into()?)
    }

    fn deserialize_u8_impl<V: Visitor<'de>>(&mut self, visitor: V) -> Result<V::Value> {
        visitor.visit_u8(self.array.next_required()?.into_u8()?)
    }

    fn deserialize_u16_impl<V: Visitor<'de>>(&mut self, visitor: V) -> Result<V::Value> {
        visitor.visit_u16(self.array.next_required()?.into_u16()?)
    }

    fn deserialize_u32_impl<V: Visitor<'de>>(&mut self, visitor: V) -> Result<V::Value> {
        visitor.visit_u32(self.array.next_required()?.into_u32()?)
    }

    fn deserialize_u64_impl<V: Visitor<'de>>(&mut self, visitor: V) -> Result<V::Value> {
        visitor.visit_u64(self.array.next_required()?.into_u64()?)
    }

    fn deserialize_i8_impl<V: Visitor<'de>>(&mut self, visitor: V) -> Result<V::Value> {
        visitor.visit_i8(self.array.next_required()?.into_i8()?)
    }

    fn deserialize_i16_impl<V: Visitor<'de>>(&mut self, visitor: V) -> Result<V::Value> {
        visitor.visit_i16(self.array.next_required()?.into_i16()?)
    }

    fn deserialize_i32_impl<V: Visitor<'de>>(&mut self, visitor: V) -> Result<V::Value> {
        visitor.visit_i32(self.array.next_required()?.into_i32()?)
    }

    fn deserialize_i64_impl<V: Visitor<'de>>(&mut self, visitor: V) -> Result<V::Value> {
        visitor.visit_i64(self.array.next_required()?.into_i64()?)
    }
}
