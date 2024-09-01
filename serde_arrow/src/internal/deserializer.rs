use serde::de::Visitor;

use crate::internal::{
    arrow::{ArrayView, Field},
    deserialization::{
        array_deserializer::ArrayDeserializer,
        outer_sequence_deserializer::OuterSequenceDeserializer,
    },
    error::{fail, Error, Result},
    schema::get_strategy_from_metadata,
    utils::array_view_ext::ArrayViewExt,
};

/// A structure to deserialize Arrow arrays into Rust objects
///
#[cfg_attr(any(has_arrow, has_arrow2), doc = r"It can be constructed via")]
#[cfg_attr(any(has_arrow, has_arrow2), doc = r"")]
#[cfg_attr(has_arrow, doc = r"- [`Deserializer::from_record_batch`]")]
#[cfg_attr(has_arrow, doc = r"- [`Deserializer::from_arrow`]")]
#[cfg_attr(has_arrow2, doc = r"- [`Deserializer::from_arrow2`]")]
pub struct Deserializer<'de>(pub(crate) OuterSequenceDeserializer<'de>);

impl<'de> Deserializer<'de> {
    pub(crate) fn new(fields: &[Field], views: Vec<ArrayView<'de>>) -> Result<Self> {
        let len = match views.first() {
            Some(view) => view.len(),
            None => 0,
        };

        let mut deserializers = Vec::new();
        for (field, view) in std::iter::zip(fields, views) {
            if view.len() != len {
                fail!("Cannot deserialize from arrays with different lengths");
            }
            let strategy = get_strategy_from_metadata(&field.metadata)?;
            let deserializer = ArrayDeserializer::new(String::from("$"), strategy.as_ref(), view)?;
            deserializers.push((field.name.clone(), deserializer));
        }

        let deserializer = OuterSequenceDeserializer::new(deserializers, len);
        let deserializer = Deserializer(deserializer);

        Ok(deserializer)
    }
}

impl<'de> serde::de::Deserializer<'de> for Deserializer<'de> {
    type Error = Error;

    fn deserialize_any<V: Visitor<'de>>(self, visitor: V) -> Result<V::Value> {
        self.deserialize_seq(visitor)
    }

    fn deserialize_ignored_any<V: Visitor<'de>>(self, visitor: V) -> Result<V::Value> {
        self.deserialize_seq(visitor)
    }

    fn deserialize_seq<V: Visitor<'de>>(self, visitor: V) -> Result<V::Value> {
        visitor.visit_seq(self.0)
    }

    fn deserialize_tuple<V: Visitor<'de>>(self, _len: usize, visitor: V) -> Result<V::Value> {
        visitor.visit_seq(self.0)
    }

    fn deserialize_tuple_struct<V: Visitor<'de>>(
        self,
        _name: &'static str,
        _len: usize,
        visitor: V,
    ) -> Result<V::Value> {
        visitor.visit_seq(self.0)
    }

    fn deserialize_newtype_struct<V: Visitor<'de>>(
        self,
        _: &'static str,
        visitor: V,
    ) -> Result<V::Value> {
        visitor.visit_newtype_struct(self)
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

    fn deserialize_unit_struct<V: Visitor<'de>>(self, _: &'static str, _: V) -> Result<V::Value> {
        fail!("Cannot deserialize single units")
    }

    fn is_human_readable(&self) -> bool {
        false
    }
}

const _: () = {
    trait AssertSendSync: Send + Sync {}
    impl<'de> AssertSendSync for Deserializer<'de> {}
};
