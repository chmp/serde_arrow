use marrow::view::BitsWithOffset;
use serde::{
    de::{SeqAccess, Visitor},
    Deserializer,
};

use crate::internal::{
    error::{fail, Error, Result},
    utils::array_ext::get_bit_buffer,
};

pub fn bitset_is_set(set: &BitsWithOffset<'_>, idx: usize) -> Result<bool> {
    get_bit_buffer(set.data, set.offset, idx)
}

pub struct U8Deserializer(pub u8);

macro_rules! unimplemented {
    ($lifetime:lifetime, $name:ident $($tt:tt)*) => {
        fn $name<V: Visitor<$lifetime>>(self $($tt)*, _: V) -> Result<V::Value> {
            fail!("Unsupported: U8Deserializer does not implement {}", stringify!($name))
        }
    };
}

impl<'de> Deserializer<'de> for U8Deserializer {
    type Error = Error;

    fn deserialize_any<V: Visitor<'de>>(self, visitor: V) -> Result<V::Value> {
        self.deserialize_u8(visitor)
    }

    fn deserialize_ignored_any<V: Visitor<'de>>(self, visitor: V) -> Result<V::Value> {
        self.deserialize_any(visitor)
    }

    fn deserialize_u8<V: Visitor<'de>>(self, visitor: V) -> Result<V::Value> {
        visitor.visit_u8(self.0)
    }

    fn deserialize_u16<V: Visitor<'de>>(self, visitor: V) -> Result<V::Value> {
        visitor.visit_u16(self.0.into())
    }

    fn deserialize_u32<V: Visitor<'de>>(self, visitor: V) -> Result<V::Value> {
        visitor.visit_u32(self.0.into())
    }

    fn deserialize_u64<V: Visitor<'de>>(self, visitor: V) -> Result<V::Value> {
        visitor.visit_u64(self.0.into())
    }

    fn deserialize_i8<V: Visitor<'de>>(self, visitor: V) -> Result<V::Value> {
        visitor.visit_i8(self.0.try_into()?)
    }

    fn deserialize_i16<V: Visitor<'de>>(self, visitor: V) -> Result<V::Value> {
        visitor.visit_i16(self.0.into())
    }

    fn deserialize_i32<V: Visitor<'de>>(self, visitor: V) -> Result<V::Value> {
        visitor.visit_i32(self.0.into())
    }

    fn deserialize_i64<V: Visitor<'de>>(self, visitor: V) -> Result<V::Value> {
        visitor.visit_i64(self.0.into())
    }

    unimplemented!('de, deserialize_identifier);
    unimplemented!('de, deserialize_str);
    unimplemented!('de, deserialize_string);
    unimplemented!('de, deserialize_bool);
    unimplemented!('de, deserialize_f32);
    unimplemented!('de, deserialize_f64);
    unimplemented!('de, deserialize_char);
    unimplemented!('de, deserialize_bytes);
    unimplemented!('de, deserialize_byte_buf);
    unimplemented!('de, deserialize_option);
    unimplemented!('de, deserialize_unit);
    unimplemented!('de, deserialize_unit_struct, _: &'static str);
    unimplemented!('de, deserialize_newtype_struct, _: &'static str);
    unimplemented!('de, deserialize_seq);
    unimplemented!('de, deserialize_tuple, _: usize);
    unimplemented!('de, deserialize_tuple_struct, _: &'static str, _: usize);
    unimplemented!('de, deserialize_map);
    unimplemented!('de, deserialize_struct, _: &'static str, _: &'static [&'static str]);
    unimplemented!('de, deserialize_enum, _: &'static str, _: &'static [&'static str]);
}

pub struct U8SliceDeserializer<'a>(&'a [u8]);

impl<'a> U8SliceDeserializer<'a> {
    pub fn new(bytes: &'a [u8]) -> Self {
        Self(bytes)
    }
}

impl<'de> SeqAccess<'de> for U8SliceDeserializer<'de> {
    type Error = Error;

    fn size_hint(&self) -> Option<usize> {
        Some(self.0.len())
    }

    fn next_element_seed<T: serde::de::DeserializeSeed<'de>>(
        &mut self,
        seed: T,
    ) -> Result<Option<T::Value>> {
        let Some((item, rest)) = self.0.split_first() else {
            return Ok(None);
        };
        let item = seed.deserialize(U8Deserializer(*item))?;
        self.0 = rest;

        Ok(Some(item))
    }
}
