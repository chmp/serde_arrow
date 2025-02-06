use marrow::view::UnionView;
use serde::de::{DeserializeSeed, Deserializer, EnumAccess, Visitor};

use crate::internal::{
    error::{fail, set_default, Context, Error, Result},
    schema::get_strategy_from_metadata,
    utils::{ChildName, Offset},
};

use super::{
    array_deserializer::ArrayDeserializer,
    random_access_deserializer::{PositionedDeserializer, RandomAccessDeserializer},
};

pub struct EnumDeserializer<'a> {
    pub path: String,
    pub types: &'a [i8],
    pub offsets: &'a [i32],
    pub variants: Vec<(String, ArrayDeserializer<'a>)>,
}

impl<'a> EnumDeserializer<'a> {
    pub fn new(path: String, view: UnionView<'a>) -> Result<Self> {
        let Some(offsets) = view.offsets else {
            fail!("Only dense unions are supported");
        };

        if view.types.len() != offsets.len() {
            fail!("Offsets and type ids must have the same length")
        }

        let mut variants = Vec::new();
        for (idx, (type_id, field_meta, field_view)) in view.fields.into_iter().enumerate() {
            // TODO: introduce translation table?
            if usize::try_from(type_id) != Ok(idx) {
                fail!("Only unions with consecutive type ids are currently supported");
            }
            let child_path = format!("{path}.{child}", child = ChildName(&field_meta.name));
            let field_deserializer = ArrayDeserializer::new(
                child_path,
                get_strategy_from_metadata(&field_meta.metadata)?.as_ref(),
                field_view,
            )?;
            variants.push((field_meta.name, field_deserializer))
        }

        Ok(Self {
            path,
            types: view.types,
            offsets,
            variants,
        })
    }
}

impl Context for EnumDeserializer<'_> {
    fn annotate(&self, annotations: &mut std::collections::BTreeMap<String, String>) {
        set_default(annotations, "field", &self.path);
        set_default(annotations, "data_type", "Union(..)");
    }
}

impl<'de> RandomAccessDeserializer<'de> for EnumDeserializer<'de> {
    fn is_some(&self, idx: usize) -> Result<bool> {
        if idx >= self.types.len() {
            fail!("Access beyond bounds");
        }
        Ok(true)
    }

    fn deserialize_any_some<V: Visitor<'de>>(&self, visitor: V, idx: usize) -> Result<V::Value> {
        self.deserialize_enum("", &[], visitor, idx)
    }

    fn deserialize_enum<V: Visitor<'de>>(
        &self,
        _: &'static str,
        _: &'static [&'static str],
        visitor: V,
        idx: usize,
    ) -> Result<V::Value> {
        if idx >= self.types.len() {
            fail!("Exhausted deserializer");
        }
        let type_id = self.types[idx];
        let offset = self.offsets[idx].try_into_usize()?;
        let (name, variant) = &self.variants[type_id as usize];

        visitor.visit_enum(VariantItemDeserializer {
            deserializer: variant.at(offset),
            type_id,
            name,
        })
    }
}

struct VariantItemDeserializer<'this, 'a> {
    deserializer: PositionedDeserializer<'this, ArrayDeserializer<'a>>,
    type_id: i8,
    name: &'this str,
}

impl<'a, 'de> EnumAccess<'de> for VariantItemDeserializer<'a, 'de> {
    type Variant = PositionedDeserializer<'a, ArrayDeserializer<'de>>;
    type Error = Error;

    fn variant_seed<V: DeserializeSeed<'de>>(self, seed: V) -> Result<(V::Value, Self::Variant)> {
        let val = seed.deserialize(VariantIdDeserializer {
            type_id: self.type_id,
            name: self.name,
        })?;
        Ok((val, self.deserializer))
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

    fn deserialize_any<V: Visitor<'de>>(self, visitor: V) -> Result<V::Value> {
        self.deserialize_str(visitor)
    }

    fn deserialize_ignored_any<V: Visitor<'de>>(self, visitor: V) -> Result<V::Value> {
        self.deserialize_any(visitor)
    }

    fn deserialize_identifier<V: Visitor<'de>>(self, visitor: V) -> Result<V::Value> {
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
}
