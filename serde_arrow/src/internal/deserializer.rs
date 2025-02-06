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
///
/// Deserializer deserializer into a sequence of records, but it can also be used a sequence of
/// deserializers for the individual records. It is possible to get individual items via
/// [`Deserializer::get`] and iterate over them via [`Deserializer::iter`].
///
/// ```rust
/// # fn main() -> serde_arrow::_impl::PanicOnError<()> {
/// use serde::Deserialize;
/// use serde_arrow::Deserializer;
/// # use serde_arrow::marrow::view::{View, PrimitiveView};
/// # use serde_arrow::marrow::datatypes::{DataType, Field};
/// # use format as error;
///
/// #[derive(Deserialize)]
/// struct Record {
///     a: i32,
///     b: f64,
/// }
///
/// # let deserializer = Deserializer::from_marrow(
/// #    &[
/// #        Field {
/// #            name: String::from("a"),
/// #            data_type: DataType::Int32,
/// #            ..Field::default()
/// #        },
/// #        Field {
/// #            name: String::from("b"),
/// #            data_type: DataType::Float32,
/// #            ..Field::default()
/// #        },
/// #    ],
/// #    &[
/// #        View::Int32(PrimitiveView {
/// #            values: &[1, 2, 3],
/// #            validity: None,
/// #        }),
/// #        View::Float32(PrimitiveView {
/// #            values: &[4.0, 5.0, 6.0],
/// #            validity: None,
/// #        }),
/// #    ]
/// # )?;
/// #
/// // iterate over the items
/// for item in &deserializer {
///     let item = Record::deserialize(item)?;
/// }
///
/// // access an item by index
/// let item = Record::deserialize(
///     deserializer.get(1).ok_or_else(|| error!("Could not get item"))?
/// )?;
/// # Ok(())
/// # }
/// ```
pub struct Deserializer<'de> {
    pub(crate) deserializer: StructDeserializer<'de>,
}

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

    /// Get the number of records of the deserializer
    pub fn len(&self) -> usize {
        self.deserializer.len
    }

    /// Check wether the deserialize contains zero records
    pub fn is_empty(&self) -> bool {
        self.len() == 0
    }

    /// Iterate over the records of this deserializer
    ///
    /// It is also possible to iterate directly over deserializer references.
    pub fn iter<'this>(&'this self) -> DeserializerIterator<'this, 'de> {
        DeserializerIterator::new(self)
    }

    /// Deserialize a single item
    pub fn get<'this>(&'this self, idx: usize) -> Option<DeserializerItem<'this, 'de>> {
        if idx >= self.deserializer.len {
            return None;
        }
        Some(DeserializerItem::new(self, idx))
    }
}

/// Iterator over the items of a deserializer
pub struct DeserializerIterator<'this, 'de> {
    deserializer: &'this StructDeserializer<'de>,
    next: usize,
}

impl<'this, 'de> DeserializerIterator<'this, 'de> {
    fn new(deserializer: &'this Deserializer<'de>) -> Self {
        Self {
            deserializer: &deserializer.deserializer,
            next: 0,
        }
    }
}

impl<'this, 'de> std::iter::IntoIterator for &'this Deserializer<'de> {
    type IntoIter = DeserializerIterator<'this, 'de>;
    type Item = DeserializerItem<'this, 'de>;

    fn into_iter(self) -> Self::IntoIter {
        DeserializerIterator::new(self)
    }
}

impl<'this, 'de> std::iter::Iterator for DeserializerIterator<'this, 'de> {
    type Item = DeserializerItem<'this, 'de>;

    fn next(&mut self) -> Option<Self::Item> {
        if self.next >= self.deserializer.len {
            return None;
        }
        let idx = self.next;
        self.next += 1;

        Some(DeserializerItem {
            deserializer: self.deserializer,
            idx,
        })
    }

    fn size_hint(&self) -> Option<usize> {
        (self.deserializer.len, Some(self.deserializer.len))
    }
}

/// Marker to hide trait implementations
struct Private<T>(T);

/// Deserialize a single item
pub struct DeserializerItem<'this, 'de> {
    deserializer: &'this StructDeserializer<'de>,
    idx: usize,
}

impl<'this, 'de> DeserializerItem<'this, 'de> {
    fn new(deserializer: &'this Deserializer<'de>, idx: usize) -> Self {
        Self {
            deserializer: &deserializer.deserializer,
            idx,
        }
    }
}

impl<'de> serde::de::Deserializer<'de> for DeserializerItem<'_, 'de> {
    type Error = Error;

    fn deserialize_any<V: Visitor<'de>>(self, visitor: V) -> Result<V::Value> {
        self.deserializer.at(self.idx).deserialize_any(visitor)
    }

    fn deserialize_ignored_any<V: Visitor<'de>>(self, visitor: V) -> Result<V::Value> {
        self.deserializer
            .at(self.idx)
            .deserialize_ignored_any(visitor)
    }

    fn deserialize_bool<V: Visitor<'de>>(self, visitor: V) -> Result<V::Value> {
        self.deserializer.at(self.idx).deserialize_bool(visitor)
    }

    fn deserialize_i8<V: Visitor<'de>>(self, visitor: V) -> Result<V::Value> {
        self.deserializer.at(self.idx).deserialize_i8(visitor)
    }

    fn deserialize_i16<V: Visitor<'de>>(self, visitor: V) -> Result<V::Value> {
        self.deserializer.at(self.idx).deserialize_i16(visitor)
    }

    fn deserialize_i32<V: Visitor<'de>>(self, visitor: V) -> Result<V::Value> {
        self.deserializer.at(self.idx).deserialize_i32(visitor)
    }

    fn deserialize_i64<V: Visitor<'de>>(self, visitor: V) -> Result<V::Value> {
        self.deserializer.at(self.idx).deserialize_i64(visitor)
    }

    fn deserialize_u8<V: Visitor<'de>>(self, visitor: V) -> Result<V::Value> {
        self.deserializer.at(self.idx).deserialize_u8(visitor)
    }

    fn deserialize_u16<V: Visitor<'de>>(self, visitor: V) -> Result<V::Value> {
        self.deserializer.at(self.idx).deserialize_u16(visitor)
    }

    fn deserialize_u32<V: Visitor<'de>>(self, visitor: V) -> Result<V::Value> {
        self.deserializer.at(self.idx).deserialize_u32(visitor)
    }

    fn deserialize_u64<V: Visitor<'de>>(self, visitor: V) -> Result<V::Value> {
        self.deserializer.at(self.idx).deserialize_u64(visitor)
    }

    fn deserialize_f32<V: Visitor<'de>>(self, visitor: V) -> Result<V::Value> {
        self.deserializer.at(self.idx).deserialize_f32(visitor)
    }

    fn deserialize_f64<V: Visitor<'de>>(self, visitor: V) -> Result<V::Value> {
        self.deserializer.at(self.idx).deserialize_f64(visitor)
    }

    fn deserialize_char<V: Visitor<'de>>(self, visitor: V) -> Result<V::Value> {
        self.deserializer.at(self.idx).deserialize_char(visitor)
    }

    fn deserialize_str<V: Visitor<'de>>(self, visitor: V) -> Result<V::Value> {
        self.deserializer.at(self.idx).deserialize_str(visitor)
    }

    fn deserialize_string<V: Visitor<'de>>(self, visitor: V) -> Result<V::Value> {
        self.deserializer.at(self.idx).deserialize_string(visitor)
    }

    fn deserialize_map<V: Visitor<'de>>(self, visitor: V) -> Result<V::Value> {
        self.deserializer.at(self.idx).deserialize_map(visitor)
    }

    fn deserialize_struct<V: Visitor<'de>>(
        self,
        name: &'static str,
        fields: &'static [&'static str],
        visitor: V,
    ) -> Result<V::Value> {
        self.deserializer
            .at(self.idx)
            .deserialize_struct(name, fields, visitor)
    }

    fn deserialize_byte_buf<V: Visitor<'de>>(self, visitor: V) -> Result<V::Value> {
        self.deserializer.at(self.idx).deserialize_byte_buf(visitor)
    }

    fn deserialize_bytes<V: Visitor<'de>>(self, visitor: V) -> Result<V::Value> {
        self.deserializer.at(self.idx).deserialize_bytes(visitor)
    }

    fn deserialize_enum<V: Visitor<'de>>(
        self,
        name: &'static str,
        variants: &'static [&'static str],
        visitor: V,
    ) -> Result<V::Value> {
        self.deserializer
            .at(self.idx)
            .deserialize_enum(name, variants, visitor)
    }

    fn deserialize_identifier<V: Visitor<'de>>(self, visitor: V) -> Result<V::Value> {
        self.deserializer
            .at(self.idx)
            .deserialize_identifier(visitor)
    }

    fn deserialize_option<V: Visitor<'de>>(self, visitor: V) -> Result<V::Value> {
        self.deserializer.at(self.idx).deserialize_option(visitor)
    }

    fn deserialize_newtype_struct<V: Visitor<'de>>(
        self,
        name: &'static str,
        visitor: V,
    ) -> Result<V::Value> {
        self.deserializer
            .at(self.idx)
            .deserialize_newtype_struct(name, visitor)
    }

    fn deserialize_tuple<V: Visitor<'de>>(self, len: usize, visitor: V) -> Result<V::Value> {
        self.deserializer
            .at(self.idx)
            .deserialize_tuple(len, visitor)
    }

    fn deserialize_seq<V: Visitor<'de>>(self, visitor: V) -> Result<V::Value> {
        self.deserializer.at(self.idx).deserialize_seq(visitor)
    }

    fn deserialize_tuple_struct<V: Visitor<'de>>(
        self,
        name: &'static str,
        len: usize,
        visitor: V,
    ) -> Result<V::Value> {
        self.deserializer
            .at(self.idx)
            .deserialize_tuple_struct(name, len, visitor)
    }

    fn deserialize_unit<V: Visitor<'de>>(self, visitor: V) -> Result<V::Value> {
        self.deserializer.at(self.idx).deserialize_unit(visitor)
    }

    fn deserialize_unit_struct<V: Visitor<'de>>(
        self,
        name: &'static str,
        visitor: V,
    ) -> Result<V::Value> {
        self.deserializer
            .at(self.idx)
            .deserialize_unit_struct(name, visitor)
    }
}

impl<'de> serde::de::Deserializer<'de> for Deserializer<'de> {
    type Error = Error;

    fn deserialize_seq<V: Visitor<'de>>(self, visitor: V) -> Result<V::Value> {
        visitor.visit_seq(Private(DeserializerIterator::new(&self)))
    }

    fn deserialize_tuple<V: Visitor<'de>>(self, _len: usize, visitor: V) -> Result<V::Value> {
        visitor.visit_seq(Private(DeserializerIterator::new(&self)))
    }

    fn deserialize_tuple_struct<V: Visitor<'de>>(
        self,
        _name: &'static str,
        _len: usize,
        visitor: V,
    ) -> Result<V::Value> {
        visitor.visit_seq(Private(DeserializerIterator::new(&self)))
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

    fn deserialize_unit_struct<V: Visitor<'de>>(self, _: &'static str, _: V) -> Result<V::Value> {
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

impl<'de> SeqAccess<'de> for Private<DeserializerIterator<'_, 'de>> {
    type Error = Error;

    fn next_element_seed<T: DeserializeSeed<'de>>(&mut self, seed: T) -> Result<Option<T::Value>> {
        if self.0.next >= self.0.deserializer.len {
            return Ok(None);
        }
        let item = seed.deserialize(self.0.deserializer.at(self.0.next))?;
        self.0.next += 1;
        Ok(Some(item))
    }
}

const _: () = {
    trait AssertSendSync: Send + Sync {}
    impl AssertSendSync for Deserializer<'_> {}
    impl AssertSendSync for DeserializerItem<'_, '_> {}
    impl AssertSendSync for DeserializerIterator<'_, '_> {}
};
