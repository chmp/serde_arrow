use std::collections::{BTreeMap, HashMap};

use marrow::{
    array::{Array, StructArray},
    datatypes::FieldMeta,
};
use serde::{Serialize, Serializer};

use crate::internal::{
    error::{fail, set_default, try_, Context, ContextSupport, Error, Result},
    serialization::utils::impl_serializer,
    utils::array_ext::{ArrayExt, CountArray, SeqArrayExt},
};

use super::array_builder::ArrayBuilder;

const UNKNOWN_KEY: usize = usize::MAX;

#[derive(Debug, Clone)]
pub struct StructBuilder {
    pub name: String,
    pub fields: Vec<ArrayBuilder>,
    pub lookup_cache: Vec<Option<StaticFieldName>>,
    pub next: usize,
    pub seen: Vec<bool>,
    pub seq: CountArray,
    pub metadata: HashMap<String, String>,
}

impl StructBuilder {
    pub fn new(
        name: String,
        fields: Vec<ArrayBuilder>,
        is_nullable: bool,
        metadata: HashMap<String, String>,
    ) -> Result<Self> {
        Ok(Self {
            name,
            seq: CountArray::new(is_nullable),
            seen: vec![false; fields.len()],
            next: 0,
            lookup_cache: vec![None; fields.len()],
            fields,
            metadata,
        })
    }

    pub fn take_self(&mut self) -> Self {
        Self {
            name: self.name.clone(),
            metadata: self.metadata.clone(),
            fields: self
                .fields
                .iter_mut()
                .map(|builder| builder.take())
                .collect(),
            lookup_cache: std::mem::replace(&mut self.lookup_cache, vec![None; self.fields.len()]),
            seen: std::mem::replace(&mut self.seen, vec![false; self.fields.len()]),
            seq: self.seq.take(),
            next: std::mem::take(&mut self.next),
        }
    }

    pub fn take(&mut self) -> ArrayBuilder {
        ArrayBuilder::Struct(self.take_self())
    }

    pub fn is_nullable(&self) -> bool {
        self.seq.validity.is_some()
    }

    pub fn into_array(self) -> Result<Array> {
        Ok(self.into_array_and_field_meta()?.0)
    }

    pub fn into_array_and_field_meta(self) -> Result<(Array, FieldMeta)> {
        let meta = FieldMeta {
            name: self.name,
            metadata: self.metadata,
            nullable: self.seq.validity.is_some(),
        };

        let mut fields = Vec::new();
        for builder in self.fields {
            let (array, meta) = builder.into_array_and_field_meta()?;
            fields.push((meta, array));
        }

        let array = Array::Struct(StructArray {
            len: self.seq.len,
            validity: self.seq.validity,
            fields,
        });
        Ok((array, meta))
    }

    pub fn reserve(&mut self, len: usize) {
        for builder in &mut self.fields {
            builder.reserve(len);
        }
    }

    pub fn serialize_default_value(&mut self) -> Result<()> {
        try_(|| {
            self.seq.push_seq_default()?;
            for builder in &mut self.fields {
                builder.serialize_default_value()?;
            }

            Ok(())
        })
        .ctx(self)
    }

    pub fn lookup(&mut self, guess: usize, key: &'static str) -> Option<usize> {
        if self.lookup_cache.get(guess) == Some(&Some(StaticFieldName(key))) {
            Some(guess)
        } else {
            let idx = self.lookup_field_uncached(key)?;
            if self.lookup_cache[idx].is_none() {
                self.lookup_cache[idx] = Some(StaticFieldName(key));
            }
            Some(idx)
        }
    }

    pub fn lookup_field_uncached(&self, name: &str) -> Option<usize> {
        for (idx, builder) in self.fields.iter().enumerate() {
            if name == builder.get_name() {
                return Some(idx);
            }
        }
        None
    }
}

impl StructBuilder {
    fn start(&mut self) -> Result<()> {
        self.seq.start_seq()?;
        self.reset();
        Ok(())
    }

    fn reset(&mut self) {
        self.seen.fill(false);
        self.next = 0;
    }

    pub fn end(&mut self) -> Result<()> {
        self.seq.end_seq()?;
        for (idx, seen) in self.seen.iter_mut().enumerate() {
            if !*seen {
                if !self.fields[idx].is_nullable() {
                    fail!(
                        "Missing non-nullable field {:?} in struct",
                        self.fields[idx].get_name(),
                    );
                }

                self.fields[idx].serialize_none()?;
            }
        }
        Ok(())
    }

    pub fn element<T: Serialize + ?Sized>(&mut self, idx: usize, value: &T) -> Result<()> {
        self.seq.push_seq_elements(1)?;
        if self.seen[idx] {
            fail!(in self, "Duplicate field {key}", key = self.fields[idx].get_name());
        }

        value.serialize(&mut self.fields[idx])?;
        self.seen[idx] = true;
        self.next = idx + 1;
        Ok(())
    }
}

impl Context for StructBuilder {
    fn annotate(&self, annotations: &mut BTreeMap<String, String>) {
        set_default(annotations, "field", &self.name);
        set_default(annotations, "data_type", "Struct(..)");
    }
}

impl<'a> Serializer for &'a mut StructBuilder {
    impl_serializer!(
        'a, StructBuilder;
        override serialize_none,
        override serialize_struct,
        override serialize_map,
        override serialize_tuple,
        override serialize_tuple_struct,
    );

    fn serialize_none(self) -> Result<()> {
        try_(|| {
            self.seq.push_seq_none()?;
            for builder in &mut self.fields {
                builder.serialize_default_value()?;
            }
            Ok(())
        })
        .ctx(self)
    }

    fn serialize_struct(self, _name: &'static str, _len: usize) -> Result<Self::SerializeStruct> {
        self.start().ctx(self)?;
        Ok(Self::SerializeStruct::Struct(self))
    }

    fn serialize_map(self, _len: Option<usize>) -> Result<Self::SerializeMap> {
        self.start().ctx(self)?;
        // always re-set to an invalid field to force that `_key()` is called before `_value()`.
        self.next = UNKNOWN_KEY;
        Ok(Self::SerializeMap::Struct(self))
    }

    fn serialize_tuple(self, _len: usize) -> Result<Self::SerializeTuple> {
        self.start().ctx(self)?;
        Ok(Self::SerializeTuple::Struct(self))
    }

    fn serialize_tuple_struct(
        self,
        _name: &'static str,
        len: usize,
    ) -> Result<Self::SerializeTupleStruct> {
        self.serialize_tuple(len)
    }
}

impl serde::ser::SerializeStruct for &mut StructBuilder {
    type Ok = ();
    type Error = Error;

    fn serialize_field<T: ?Sized + serde::Serialize>(
        &mut self,
        key: &'static str,
        value: &T,
    ) -> Result<()> {
        let Some(idx) = self.lookup(self.next, key) else {
            // ignore unknown fields
            return Ok(());
        };
        self.element(idx, value)
    }

    fn end(self) -> Result<()> {
        StructBuilder::end(self)
    }
}

impl serde::ser::SerializeMap for &mut StructBuilder {
    type Ok = ();
    type Error = Error;

    fn serialize_key<T: ?Sized + Serialize>(&mut self, key: &T) -> Result<()> {
        self.next = KeyLookupSerializer::lookup(&self.fields, key)
            .ctx(*self)?
            .unwrap_or(UNKNOWN_KEY);
        Ok(())
    }

    fn serialize_value<T: ?Sized + Serialize>(&mut self, value: &T) -> Result<()> {
        if self.next != UNKNOWN_KEY {
            self.element(self.next, value).ctx(*self)?;
        }
        self.next = UNKNOWN_KEY;
        Ok(())
    }

    fn end(self) -> Result<()> {
        StructBuilder::end(&mut *self).ctx(self)
    }
}

impl serde::ser::SerializeTuple for &mut StructBuilder {
    type Ok = ();
    type Error = Error;

    fn serialize_element<T: ?Sized + Serialize>(&mut self, value: &T) -> Result<()> {
        try_(|| {
            // ignore extra tuple fields
            if self.next < self.fields.len() {
                self.element(self.next, value)?;
            }
            Ok(())
        })
        .ctx(*self)
    }

    fn end(self) -> Result<()> {
        self.end().ctx(self)
    }
}

/// A wrapper around a static field name that compares using ptr and length
#[derive(Debug, Clone)]
pub struct StaticFieldName(&'static str);

impl std::cmp::PartialEq for StaticFieldName {
    fn eq(&self, other: &Self) -> bool {
        (self.0.as_ptr(), self.0.len()) == (other.0.as_ptr(), other.0.len())
    }
}

#[derive(Debug)]
pub struct KeyLookupSerializer<'a> {
    fields: &'a [ArrayBuilder],
    result: Option<usize>,
}

impl<'a> KeyLookupSerializer<'a> {
    pub fn lookup<K: Serialize + ?Sized>(
        fields: &'a [ArrayBuilder],
        key: &K,
    ) -> Result<Option<usize>> {
        let mut this = Self {
            fields,
            result: None,
        };
        key.serialize(&mut this)?;
        Ok(this.result)
    }
}

impl Context for KeyLookupSerializer<'_> {
    fn annotate(&self, _: &mut BTreeMap<String, String>) {}
}

impl<'a> Serializer for &'a mut KeyLookupSerializer<'_> {
    impl_serializer!(
        'a, KeyLookupSerializer;
        override serialize_str,
    );

    fn serialize_str(self, v: &str) -> Result<()> {
        for (idx, builder) in self.fields.iter().enumerate() {
            if builder.get_name() == v {
                self.result = Some(idx);
            }
        }
        Ok(())
    }
}
