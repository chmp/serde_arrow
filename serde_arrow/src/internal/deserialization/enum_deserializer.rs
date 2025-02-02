use std::collections::{BTreeMap, HashMap};

use serde::de::{DeserializeSeed, Deserializer, EnumAccess, Visitor};

use crate::internal::{
    error::{fail, set_default, try_, Context, ContextSupport, Error, Result},
    utils::{Mut, Offset},
};

use super::{
    array_deserializer::ArrayDeserializer,
    random_access_deserializer::{PositionedDeserializer, RandomAccessDeserializer},
    simple_deserializer::SimpleDeserializer,
};

pub struct EnumDeserializer<'a> {
    pub path: String,
    pub type_ids: &'a [i8],
    pub offsets: &'a [i32],
    pub variants: Vec<(String, ArrayDeserializer<'a>)>,
    pub next: usize,
}

impl<'a> EnumDeserializer<'a> {
    pub fn new(
        path: String,
        type_ids: &'a [i8],
        offsets: &'a [i32],
        mut variants: Vec<(String, ArrayDeserializer<'a>)>,
    ) -> Result<Self> {
        let initial_offsets = verify_offsets(type_ids, offsets, variants.len())?;

        for (type_id, initial_offset) in initial_offsets {
            let Some((_, variant)) = variants.get_mut(type_id as usize) else {
                fail!("Unexpected error: could not retrieve variant {type_id}");
            };
            variant.skip(initial_offset as usize)?;
        }

        Ok(Self {
            path,
            type_ids,
            offsets,
            variants,
            next: 0,
        })
    }
}

fn verify_offsets(type_ids: &[i8], offsets: &[i32], num_fields: usize) -> Result<HashMap<i8, i32>> {
    if type_ids.len() != offsets.len() {
        fail!("Offsets and type ids must have the same length")
    }

    for &type_id in type_ids {
        if type_id as usize >= num_fields {
            fail!(
                concat!(
                    "Invalid enum array:",
                    "type id ({type_id}) larger the number of fields ({num_fields})",
                ),
                type_id = type_id,
                num_fields = num_fields,
            );
        }
    }

    let mut last_offsets = HashMap::<i8, i32>::new();
    let mut initial_offsets = HashMap::<i8, i32>::new();
    for (idx, (&type_id, &offset)) in std::iter::zip(type_ids, offsets).enumerate() {
        if offset < 0 {
            fail!(
                concat!(
                    "Invalid offsets in enum array for item {idx}:",
                    "negative offsets ({offset}) is not supports",
                ),
                idx = idx,
                offset = offset,
            );
        }
        if let Some(last_offset) = last_offsets.get(&type_id).copied() {
            if offset.checked_sub(last_offset) != Some(1) {
                fail!(
                    concat!(
                        "Invalid offsets in enum array for item {idx}:",
                        "serde_arrow only supports consecutive offsets.",
                        "Current offset for type {type_id}: {offset}, previous offset {last_offset}",
                    ),
                    idx = idx,
                    type_id = type_id,
                    offset = offset,
                    last_offset = last_offset,
                );
            }
            last_offsets.insert(type_id, offset);
        } else {
            initial_offsets.insert(type_id, offset);
            last_offsets.insert(type_id, offset);
        }
    }

    Ok(initial_offsets)
}

impl Context for EnumDeserializer<'_> {
    fn annotate(&self, annotations: &mut std::collections::BTreeMap<String, String>) {
        set_default(annotations, "field", &self.path);
        set_default(annotations, "data_type", "Union(..)");
    }
}

impl<'de> SimpleDeserializer<'de> for EnumDeserializer<'de> {
    fn deserialize_enum<V: Visitor<'de>>(
        &mut self,
        _: &'static str,
        _: &'static [&'static str],
        visitor: V,
    ) -> Result<V::Value> {
        let mut ctx = BTreeMap::new();
        self.annotate(&mut ctx);

        try_(|| visitor.visit_enum(self)).ctx(&ctx)
    }
}

impl<'a, 'de> EnumAccess<'de> for &'a mut EnumDeserializer<'de> {
    type Variant = Mut<'a, ArrayDeserializer<'de>>;
    type Error = Error;

    fn variant_seed<V: DeserializeSeed<'de>>(self, seed: V) -> Result<(V::Value, Self::Variant)> {
        if self.next >= self.type_ids.len() {
            fail!("Exhausted deserializer");
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
            fail!("Unsupported: EnumDeserializer does not implement {}", stringify!($name))
        }
    };
}

impl<'de> Deserializer<'de> for VariantIdDeserializer<'_> {
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

impl<'de> RandomAccessDeserializer<'de> for EnumDeserializer<'de> {
    fn is_some(&self, idx: usize) -> Result<bool> {
        if idx >= self.type_ids.len() {
            fail!("Access beyond bounds");
        }
        Ok(true)
    }

    fn deserialize_any_some<V: Visitor<'de>>(&self, visitor: V, idx: usize) -> Result<V::Value> {
        visitor.visit_enum(self.at(idx))
    }
}

impl<'a, 'de> EnumAccess<'de> for PositionedDeserializer<'a, EnumDeserializer<'de>> {
    type Variant = PositionedDeserializer<'a, ArrayDeserializer<'de>>;
    type Error = Error;

    fn variant_seed<V: DeserializeSeed<'de>>(self, seed: V) -> Result<(V::Value, Self::Variant)> {
        let PositionedDeserializer(this, idx) = self;
        if idx >= this.type_ids.len() {
            fail!("Exhausted deserializer");
        }
        let type_id = this.type_ids[idx];
        let offset = this.offsets[idx].try_into_usize()?;
        let (name, variant) = &this.variants[type_id as usize];

        let val = seed.deserialize(VariantIdDeserializer { type_id, name })?;
        Ok((val, variant.at(offset)))
    }
}
