use std::sync::atomic::{AtomicPtr, Ordering};

use marrow::view::{BitsWithOffset, StructView};
use serde::de::{value::StrDeserializer, DeserializeSeed, MapAccess, SeqAccess, Visitor};

use crate::internal::{
    error::{fail, set_default, Context, ContextSupport, Error, Result},
    schema::get_strategy_from_metadata,
    utils::ChildName,
};

use super::{
    array_deserializer::ArrayDeserializer, random_access_deserializer::RandomAccessDeserializer,
    utils::bitset_is_set,
};

pub struct StructDeserializer<'a> {
    pub path: String,
    pub fields: Vec<(String, ArrayDeserializer<'a>)>,
    pub validity: Option<BitsWithOffset<'a>>,
    pub len: usize,
    ordered_fields: AtomicPtr<&'static str>,
}

impl<'a> StructDeserializer<'a> {
    pub fn new(path: String, view: StructView<'a>) -> Result<Self> {
        let mut fields = Vec::with_capacity(view.fields.len());
        for (field_meta, field_view) in view.fields {
            let child_path = format!("{path}.{child}", child = ChildName(&field_meta.name));
            let field_deserializer = ArrayDeserializer::new(
                child_path,
                get_strategy_from_metadata(&field_meta.metadata)?.as_ref(),
                field_view,
            )?;
            let field_name = field_meta.name;

            fields.push((field_name, field_deserializer));
        }

        Ok(Self::from_parts(path, fields, view.validity, view.len))
    }

    pub fn from_parts(
        path: String,
        fields: Vec<(String, ArrayDeserializer<'a>)>,
        validity: Option<BitsWithOffset<'a>>,
        len: usize,
    ) -> Self {
        Self {
            path,
            fields,
            validity,
            len,
            ordered_fields: AtomicPtr::new(std::ptr::null_mut()),
        }
    }

    fn fields_match(&self, fields: &'static [&'static str]) -> bool {
        if self.fields.len() != fields.len() {
            return false;
        }

        // Derived Deserialize implementations use a static field slice. Cache the
        // matching slice's address so subsequent rows skip string comparisons.
        let field_ptr = fields.as_ptr() as *mut &'static str;
        if self.ordered_fields.load(Ordering::Relaxed) == field_ptr {
            return true;
        }

        if self
            .fields
            .iter()
            .zip(fields)
            .all(|((actual, _), expected)| actual == expected)
        {
            self.ordered_fields.store(field_ptr, Ordering::Relaxed);
            true
        } else {
            false
        }
    }
}

impl Context for StructDeserializer<'_> {
    fn annotate(&self, annotations: &mut std::collections::BTreeMap<String, String>) {
        set_default(annotations, "field", &self.path);
        set_default(annotations, "data_type", "Struct");
    }
}

impl<'de> RandomAccessDeserializer<'de> for StructDeserializer<'de> {
    fn is_some(&self, idx: usize) -> Result<bool> {
        if idx >= self.len {
            fail!(
                "index {idx} is out of bounds for Struct array with length {}",
                self.len
            );
        }
        if let Some(validity) = self.validity.as_ref() {
            Ok(bitset_is_set(validity, idx)?)
        } else {
            Ok(true)
        }
    }

    fn deserialize_any_some<V: Visitor<'de>>(&self, visitor: V, idx: usize) -> Result<V::Value> {
        if idx >= self.len {
            fail!(
                "index {idx} is out of bounds for Struct array with length {}",
                self.len
            );
        }
        visitor.visit_map(StructItemDeserializer::new(self, idx))
    }

    fn deserialize_map<V: Visitor<'de>>(&self, visitor: V, idx: usize) -> Result<V::Value> {
        visitor
            .visit_map(StructItemDeserializer::new(self, idx))
            .ctx(self)
    }

    fn deserialize_struct<V: Visitor<'de>>(
        &self,
        _: &'static str,
        fields: &'static [&'static str],
        visitor: V,
        idx: usize,
    ) -> Result<V::Value> {
        if self.fields_match(fields) {
            visitor
                .visit_seq(StructSeqDeserializer::new(self, idx))
                .ctx(self)
        } else {
            visitor
                .visit_map(StructItemDeserializer::new(self, idx))
                .ctx(self)
        }
    }

    fn deserialize_tuple<V: Visitor<'de>>(
        &self,
        _: usize,
        visitor: V,
        idx: usize,
    ) -> Result<V::Value> {
        visitor
            .visit_seq(StructSeqDeserializer::new(self, idx))
            .ctx(self)
    }

    fn deserialize_tuple_struct<V: Visitor<'de>>(
        &self,
        _: &'static str,
        _: usize,
        visitor: V,
        idx: usize,
    ) -> Result<V::Value> {
        visitor
            .visit_seq(StructSeqDeserializer::new(self, idx))
            .ctx(self)
    }
}

struct StructItemDeserializer<'a, 'de> {
    deserializer: &'a StructDeserializer<'de>,
    item: usize,
    field: usize,
}

impl<'a, 'de> StructItemDeserializer<'a, 'de> {
    pub fn new(deserializer: &'a StructDeserializer<'de>, item: usize) -> Self {
        Self {
            deserializer,
            item,
            field: 0,
        }
    }
}

impl<'de> MapAccess<'de> for StructItemDeserializer<'_, 'de> {
    type Error = Error;

    fn next_key_seed<K: DeserializeSeed<'de>>(&mut self, seed: K) -> Result<Option<K::Value>> {
        let Some((field_name, _)) = self.deserializer.fields.get(self.field) else {
            return Ok(None);
        };

        let key = seed.deserialize(StrDeserializer::<Error>::new(field_name))?;
        Ok(Some(key))
    }

    fn next_value_seed<V: DeserializeSeed<'de>>(&mut self, seed: V) -> Result<V::Value> {
        let Some((_, field_deserializer)) = self.deserializer.fields.get(self.field) else {
            fail!(
                "next_value_seed called without a remaining struct field at position {}",
                self.field
            );
        };

        let res = seed.deserialize(field_deserializer.at(self.item))?;
        self.field += 1;

        Ok(res)
    }
}

struct StructSeqDeserializer<'a, 'de> {
    fields: &'a [(String, ArrayDeserializer<'de>)],
    item: usize,
}

impl<'a, 'de> StructSeqDeserializer<'a, 'de> {
    fn new(deserializer: &'a StructDeserializer<'de>, item: usize) -> Self {
        Self {
            fields: &deserializer.fields,
            item,
        }
    }
}

impl<'de> SeqAccess<'de> for StructSeqDeserializer<'_, 'de> {
    type Error = Error;

    fn next_element_seed<T: DeserializeSeed<'de>>(&mut self, seed: T) -> Result<Option<T::Value>> {
        let Some(((_, field_deserializer), rest)) = self.fields.split_first() else {
            return Ok(None);
        };

        let res = seed.deserialize(field_deserializer.at(self.item))?;
        self.fields = rest;

        Ok(Some(res))
    }
}

#[cfg(test)]
mod tests {
    use marrow::{
        datatypes::{DataType, Field},
        view::{PrimitiveView, View},
    };
    use serde::Deserialize;

    use crate::Deserializer;

    #[derive(Debug, Deserialize, PartialEq)]
    struct Ordered {
        a: i32,
        b: i32,
    }

    #[derive(Debug, Deserialize, PartialEq)]
    struct Reordered {
        b: i32,
        a: i32,
    }

    #[test]
    fn cached_field_match_preserves_reordered_deserialization() {
        let fields = [
            Field {
                name: "a".into(),
                data_type: DataType::Int32,
                ..Field::default()
            },
            Field {
                name: "b".into(),
                data_type: DataType::Int32,
                ..Field::default()
            },
        ];
        let views = [
            View::Int32(PrimitiveView {
                values: &[11, 33],
                validity: None,
            }),
            View::Int32(PrimitiveView {
                values: &[22, 44],
                validity: None,
            }),
        ];
        let deserializer = Deserializer::from_marrow(&fields, &views).unwrap();

        assert_eq!(
            Ordered::deserialize(deserializer.get(0).unwrap()).unwrap(),
            Ordered { a: 11, b: 22 }
        );
        assert_eq!(
            Reordered::deserialize(deserializer.get(0).unwrap()).unwrap(),
            Reordered { a: 11, b: 22 }
        );
        assert_eq!(
            Ordered::deserialize(deserializer.get(1).unwrap()).unwrap(),
            Ordered { a: 33, b: 44 }
        );
    }
}
