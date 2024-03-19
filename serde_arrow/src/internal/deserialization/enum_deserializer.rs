use serde::de::{DeserializeSeed, Deserializer, EnumAccess, Visitor};

use crate::internal::{
    common::Mut,
    error::{fail, Error, Result},
};

use super::{array_deserializer::ArrayDeserializer, simple_deserializer::SimpleDeserializer};

pub struct EnumDeserializer<'a> {
    pub type_ids: &'a [i8],
    pub variants: Vec<(String, ArrayDeserializer<'a>)>,
    pub next: usize,
}

impl<'a> EnumDeserializer<'a> {
    pub fn new(type_ids: &'a [i8], variants: Vec<(String, ArrayDeserializer<'a>)>) -> Self {
        Self {
            type_ids,
            variants,
            next: 0,
        }
    }
}

impl<'de> SimpleDeserializer<'de> for EnumDeserializer<'de> {
    fn name() -> &'static str {
        "EnumDeserializer"
    }

    fn deserialize_enum<V: Visitor<'de>>(
        &mut self,
        _: &'static str,
        _: &'static [&'static str],
        visitor: V,
    ) -> Result<V::Value> {
        visitor.visit_enum(self)
    }
}

impl<'a, 'de> EnumAccess<'de> for &'a mut EnumDeserializer<'de> {
    type Variant = Mut<'a, ArrayDeserializer<'de>>;
    type Error = Error;

    fn variant_seed<V: DeserializeSeed<'de>>(self, seed: V) -> Result<(V::Value, Self::Variant)> {
        if self.next >= self.type_ids.len() {
            fail!("Exhausted EnumDeserializer");
        }
        let type_id = self.type_ids[self.next];
        self.next += 1;

        let (name, variant) = &mut self.variants[type_id as usize];

        let val = seed.deserialize(VariantIdDeserializer { type_id, name })?;

        Ok((val, Mut(variant)))
    }
}

struct VariantIdDeserializer<'a> {
    type_id: i8,
    name: &'a str,
}

macro_rules! unimplemented {
    ($lifetime:lifetime, $name:ident $($tt:tt)*) => {
        fn $name<V: Visitor<$lifetime>>(self $($tt)*, _: V) -> Result<V::Value> {
            fail!("{} is not implemented", stringify!($name))
        }
    };
}

impl<'de, 'a> Deserializer<'de> for VariantIdDeserializer<'a> {
    type Error = Error;

    fn deserialize_identifier<V: Visitor<'de>>(self, visitor: V) -> Result<V::Value> {
        self.deserialize_str(visitor)
    }

    fn deserialize_any<V: Visitor<'de>>(self, visitor: V) -> Result<V::Value> {
        self.deserialize_str(visitor)
    }

    fn deserialize_str<V: Visitor<'de>>(self, visitor: V) -> Result<V::Value> {
        visitor.visit_str(self.name)
    }

    fn deserialize_string<V: Visitor<'de>>(self, visitor: V) -> Result<V::Value> {
        visitor.visit_string(self.name.to_owned())
    }

    fn deserialize_u64<V: Visitor<'de>>(self, visitor: V) -> Result<V::Value> {
        visitor.visit_u64(u64::try_from(self.type_id)?)
    }

    unimplemented!('de, deserialize_bool);
    unimplemented!('de, deserialize_i8);
    unimplemented!('de, deserialize_i16);
    unimplemented!('de, deserialize_i32);
    unimplemented!('de, deserialize_i64);
    unimplemented!('de, deserialize_u8);
    unimplemented!('de, deserialize_u16);
    unimplemented!('de, deserialize_u32);
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
    unimplemented!('de, deserialize_ignored_any);
}
