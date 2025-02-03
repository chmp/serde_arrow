use std::ops::Range;

use serde::de::{DeserializeSeed, SeqAccess, Visitor};

use marrow::{datatypes::Field, view::View};

use crate::internal::{
    deserialization::{
        array_deserializer::ArrayDeserializer, struct_deserializer::StructDeserializer,
    },
    error::{fail, Error, Result},
    schema::get_strategy_from_metadata,
    utils::array_view_ext::ViewExt,
};

use super::{
    deserialization::random_access_deserializer::RandomAccessDeserializer, utils::ChildName,
};

/// A structure to deserialize Arrow arrays into Rust objects
///
#[cfg_attr(any(has_arrow, has_arrow2), doc = r"It can be constructed via")]
#[cfg_attr(any(has_arrow, has_arrow2), doc = r"")]
#[cfg_attr(has_arrow, doc = r"- [`Deserializer::from_record_batch`]")]
#[cfg_attr(has_arrow, doc = r"- [`Deserializer::from_arrow`]")]
#[cfg_attr(has_arrow2, doc = r"- [`Deserializer::from_arrow2`]")]
pub struct Deserializer<'de> {
    pub(crate) deserializer: StructDeserializer<'de>,
}

#[derive(Clone, Copy)]
pub struct DeserializerSlice<'a, 'de> {
    pub(crate) deserializer: &'a StructDeserializer<'de>,
    pub(crate) start: usize,
    pub(crate) end: usize,
}

// marker to hide impl of Serde traits
pub struct Private<T>(T);

impl<'de> Deserializer<'de> {
    pub(crate) fn new(fields: &[Field], views: Vec<View<'de>>) -> Result<Self> {
        let len = match views.first() {
            Some(view) => view.len()?,
            None => 0,
        };

        let mut deserializers = Vec::new();
        for (field, view) in std::iter::zip(fields, views) {
            if view.len()? != len {
                fail!("Cannot deserialize from arrays with different lengths");
            }
            let strategy = get_strategy_from_metadata(&field.metadata)?;
            let deserializer = ArrayDeserializer::new(
                format!("$.{child}", child = ChildName(&field.name)),
                strategy.as_ref(),
                view,
            )?;
            deserializers.push((field.name.clone(), deserializer));
        }

        let deserializer =
            StructDeserializer::from_parts(String::from("$"), deserializers, None, len);

        Ok(Self { deserializer })
    }

    pub fn len(&self) -> usize {
        self.deserializer.len
    }

    pub fn is_empty(&self) -> bool {
        self.len() == 0
    }

    pub fn slice<'this>(&'this self, slice: Range<usize>) -> DeserializerSlice<'this, 'de> {
        DeserializerSlice {
            deserializer: &self.deserializer,
            start: slice.start,
            end: slice.end,
        }
    }
}

macro_rules! impl_deserializer {
    ($ty:ident) => {
        impl<'de> serde::de::Deserializer<'de> for $ty<'de> {
            type Error = Error;

            fn deserialize_seq<V: Visitor<'de>>(self, visitor: V) -> Result<V::Value> {
                visitor.visit_seq(Private(self.slice(0..self.len())))
            }

            fn deserialize_tuple<V: Visitor<'de>>(
                self,
                _len: usize,
                visitor: V,
            ) -> Result<V::Value> {
                visitor.visit_seq(Private(self.slice(0..self.len())))
            }

            fn deserialize_tuple_struct<V: Visitor<'de>>(
                self,
                _name: &'static str,
                _len: usize,
                visitor: V,
            ) -> Result<V::Value> {
                visitor.visit_seq(Private(self.slice(0..self.len())))
            }

            fn deserialize_any<V: Visitor<'de>>(self, visitor: V) -> Result<V::Value> {
                self.deserialize_seq(visitor)
            }

            fn deserialize_ignored_any<V: Visitor<'de>>(self, visitor: V) -> Result<V::Value> {
                self.deserialize_seq(visitor)
            }

            fn deserialize_bool<V: Visitor<'de>>(self, _: V) -> Result<V::Value> {
                fail!("Cannot deserialize single bools")
            }

            fn deserialize_byte_buf<V: Visitor<'de>>(self, _: V) -> Result<V::Value> {
                fail!("Cannot deserialize byte buffers")
            }

            fn deserialize_bytes<V: Visitor<'de>>(self, _: V) -> Result<V::Value> {
                fail!("Cannot deserialize byte arrays")
            }

            fn deserialize_char<V: Visitor<'de>>(self, _: V) -> Result<V::Value> {
                fail!("Cannot deserialize single chars")
            }

            fn deserialize_enum<V: Visitor<'de>>(
                self,
                _: &'static str,
                _: &'static [&'static str],
                _: V,
            ) -> Result<V::Value> {
                fail!("Cannot deserialize single enums")
            }

            fn deserialize_f32<V: Visitor<'de>>(self, _: V) -> Result<V::Value> {
                fail!("Cannot deserialize single floats")
            }

            fn deserialize_f64<V: Visitor<'de>>(self, _: V) -> Result<V::Value> {
                fail!("Cannot deserialize single floats")
            }

            fn deserialize_i128<V: Visitor<'de>>(self, _: V) -> Result<V::Value> {
                fail!("Cannot deserialize single integers")
            }

            fn deserialize_i16<V: Visitor<'de>>(self, _: V) -> Result<V::Value> {
                fail!("Cannot deserialize single integers")
            }

            fn deserialize_i32<V: Visitor<'de>>(self, _: V) -> Result<V::Value> {
                fail!("Cannot deserialize single integers")
            }

            fn deserialize_i64<V: Visitor<'de>>(self, _: V) -> Result<V::Value> {
                fail!("Cannot deserialize single integers")
            }

            fn deserialize_i8<V: Visitor<'de>>(self, _: V) -> Result<V::Value> {
                fail!("Cannot deserialize single integers")
            }

            fn deserialize_identifier<V: Visitor<'de>>(self, _: V) -> Result<V::Value> {
                fail!("Cannot deserialize single identifiers")
            }

            fn deserialize_map<V: Visitor<'de>>(self, _: V) -> Result<V::Value> {
                fail!("Cannot deserialize single maps")
            }

            fn deserialize_option<V: Visitor<'de>>(self, _: V) -> Result<V::Value> {
                fail!("Cannot deserialize single options")
            }

            fn deserialize_str<V: Visitor<'de>>(self, _: V) -> Result<V::Value> {
                fail!("Cannot deserialize single strings")
            }

            fn deserialize_string<V: Visitor<'de>>(self, _: V) -> Result<V::Value> {
                fail!("Cannot deserialize single strings")
            }

            fn deserialize_struct<V: Visitor<'de>>(
                self,
                _: &'static str,
                _: &'static [&'static str],
                _: V,
            ) -> Result<V::Value> {
                fail!("Cannot deserialize single structs")
            }

            fn deserialize_u128<V: Visitor<'de>>(self, _: V) -> Result<V::Value> {
                fail!("Cannot deserialize single integers")
            }

            fn deserialize_u16<V: Visitor<'de>>(self, _: V) -> Result<V::Value> {
                fail!("Cannot deserialize single integers")
            }

            fn deserialize_u32<V: Visitor<'de>>(self, _: V) -> Result<V::Value> {
                fail!("Cannot deserialize single integers")
            }

            fn deserialize_u64<V: Visitor<'de>>(self, _: V) -> Result<V::Value> {
                fail!("Cannot deserialize single integers")
            }

            fn deserialize_u8<V: Visitor<'de>>(self, _: V) -> Result<V::Value> {
                fail!("Cannot deserialize single integers")
            }

            fn deserialize_unit<V: Visitor<'de>>(self, _: V) -> Result<V::Value> {
                fail!("Cannot deserialize single units")
            }

            fn deserialize_unit_struct<V: Visitor<'de>>(
                self,
                _: &'static str,
                _: V,
            ) -> Result<V::Value> {
                fail!("Cannot deserialize single units")
            }

            fn deserialize_newtype_struct<V: Visitor<'de>>(
                self,
                _: &'static str,
                visitor: V,
            ) -> Result<V::Value> {
                visitor.visit_newtype_struct(self)
            }

            fn is_human_readable(&self) -> bool {
                false
            }
        }
    };
}

impl_deserializer!(Deserializer);

// TODO: expose this or hide behind private marker?
impl<'de> SeqAccess<'de> for Private<DeserializerSlice<'_, 'de>> {
    type Error = Error;

    fn next_element_seed<T: DeserializeSeed<'de>>(&mut self, seed: T) -> Result<Option<T::Value>> {
        if self.0.start >= self.0.end {
            return Ok(None);
        }

        let item = seed.deserialize(self.0.deserializer.at(self.0.start))?;
        self.0.start += 1;
        Ok(Some(item))
    }
}

const _: () = {
    trait AssertSendSync: Send + Sync {}
    impl AssertSendSync for Deserializer<'_> {}
    impl AssertSendSync for DeserializerSlice<'_, '_> {}
};
