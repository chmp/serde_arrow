use std::collections::BTreeMap;

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
    pub path: String,
    pub fields: Vec<(ArrayBuilder, FieldMeta)>,
    pub lookup_cache: Vec<Option<StaticFieldName>>,
    pub next: usize,
    pub seen: Vec<bool>,
    pub seq: CountArray,
}

impl StructBuilder {
    pub fn new(
        path: String,
        fields: Vec<(ArrayBuilder, FieldMeta)>,
        is_nullable: bool,
    ) -> Result<Self> {
        Ok(Self {
            path,
            seq: CountArray::new(is_nullable),
            seen: vec![false; fields.len()],
            next: 0,
            lookup_cache: vec![None; fields.len()],
            fields,
        })
    }

    pub fn take_self(&mut self) -> Self {
        Self {
            path: self.path.clone(),
            fields: self
                .fields
                .iter_mut()
                .map(|(builder, meta)| (builder.take(), meta.clone()))
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
        let mut fields = Vec::new();
        for (builder, meta) in self.fields {
            fields.push((meta, builder.into_array()?));
        }

        Ok(Array::Struct(StructArray {
            len: self.seq.len,
            validity: self.seq.validity,
            fields,
        }))
    }

    pub fn reserve(&mut self, len: usize) {
        for (field, _) in &mut self.fields {
            field.reserve(len);
        }
    }

    pub fn serialize_default_value(&mut self) -> Result<()> {
        try_(|| {
            self.seq.push_seq_default()?;
            for (builder, _) in &mut self.fields {
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
            if self.lookup_cache[guess].is_none() {
                self.lookup_cache[guess] = Some(StaticFieldName(key));
            }
            Some(idx)
        }
    }

    pub fn lookup_field_uncached(&self, name: &str) -> Option<usize> {
        for (idx, (_, meta)) in self.fields.iter().enumerate() {
            if name == meta.name {
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
                if !self.fields[idx].1.nullable {
                    fail!(
                        "Missing non-nullable field {:?} in struct",
                        self.fields[idx].1.name
                    );
                }

                self.fields[idx].0.serialize_none()?;
            }
        }
        Ok(())
    }

    pub fn element<T: Serialize + ?Sized>(&mut self, idx: usize, value: &T) -> Result<()> {
        self.seq.push_seq_elements(1)?;
        if self.seen[idx] {
            fail!(in self, "Duplicate field {key}", key = self.fields[idx].1.name);
        }

        value.serialize(&mut self.fields[idx].0)?;
        self.seen[idx] = true;
        self.next = idx + 1;
        Ok(())
    }
}

impl Context for StructBuilder {
    fn annotate(&self, annotations: &mut BTreeMap<String, String>) {
        set_default(annotations, "field", &self.path);
        set_default(annotations, "data_type", "Struct(..)");
    }
}

impl super::simple_serializer::SimpleSerializer for StructBuilder {
    fn serialize_default(&mut self) -> Result<()> {
        self.serialize_default_value()
    }

    fn serialize_none(&mut self) -> Result<()> {
        try_(|| {
            self.seq.push_seq_none()?;
            for (builder, _) in &mut self.fields {
                builder.serialize_default()?;
            }
            Ok(())
        })
        .ctx(self)
    }

    fn serialize_struct_start(&mut self, _: &'static str, _: usize) -> Result<()> {
        try_(|| self.start()).ctx(self)
    }

    fn serialize_struct_field<T: Serialize + ?Sized>(
        &mut self,
        key: &'static str,
        value: &T,
    ) -> Result<()> {
        try_(|| {
            let Some(idx) = self.lookup(self.next, key) else {
                // ignore unknown fields
                return Ok(());
            };
            self.element(idx, value)
        })
        .ctx(self)
    }

    fn serialize_struct_end(&mut self) -> Result<()> {
        try_(|| self.end()).ctx(self)
    }

    fn serialize_tuple_start(&mut self, _: usize) -> Result<()> {
        try_(|| self.start()).ctx(self)
    }

    fn serialize_tuple_element<V: Serialize + ?Sized>(&mut self, value: &V) -> Result<()> {
        try_(|| self.element(self.next, value)).ctx(self)
    }

    fn serialize_tuple_end(&mut self) -> Result<()> {
        try_(|| self.end()).ctx(self)
    }

    fn serialize_tuple_struct_start(&mut self, _: &'static str, _: usize) -> Result<()> {
        try_(|| self.start()).ctx(self)
    }

    fn serialize_tuple_struct_field<V: Serialize + ?Sized>(&mut self, value: &V) -> Result<()> {
        try_(|| {
            // ignore extra tuple fields
            if self.next < self.fields.len() {
                self.element(self.next, value)?;
            }
            Ok(())
        })
        .ctx(self)
    }

    fn serialize_tuple_struct_end(&mut self) -> Result<()> {
        try_(|| self.end()).ctx(self)
    }

    fn serialize_map_start(&mut self, _: Option<usize>) -> Result<()> {
        try_(|| {
            self.start()?;
            // always re-set to an invalid field to force that `_key()` is called before `_value()`.
            self.next = UNKNOWN_KEY;
            Ok(())
        })
        .ctx(self)
    }

    fn serialize_map_key<V: Serialize + ?Sized>(&mut self, key: &V) -> Result<()> {
        try_(|| {
            self.next = KeyLookupSerializer::lookup(&self.fields, key)?.unwrap_or(UNKNOWN_KEY);
            Ok(())
        })
        .ctx(self)
    }

    fn serialize_map_value<V: Serialize + ?Sized>(&mut self, value: &V) -> Result<()> {
        try_(|| {
            if self.next != UNKNOWN_KEY {
                self.element(self.next, value)?;
            }
            // see serialize_map_start
            self.next = UNKNOWN_KEY;
            Ok(())
        })
        .ctx(self)
    }

    fn serialize_map_end(&mut self) -> Result<()> {
        try_(|| self.end()).ctx(self)
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
            for (builder, _) in &mut self.fields {
                builder.serialize_default_value()?;
            }
            Ok(())
        })
        .ctx(self)
    }

    fn serialize_struct(self, _name: &'static str, _len: usize) -> Result<Self::SerializeStruct> {
        StructBuilder::start(self).ctx(self)?;
        Ok(Self::SerializeStruct::Struct(self))
    }

    fn serialize_map(self, _len: Option<usize>) -> Result<Self::SerializeMap> {
        StructBuilder::start(self).ctx(self)?;
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
        try_(|| {
            self.next = KeyLookupSerializer::lookup(&self.fields, key)?.unwrap_or(UNKNOWN_KEY);
            Ok(())
        })
        .ctx(*self)
    }

    fn serialize_value<T: ?Sized + Serialize>(&mut self, value: &T) -> Result<()> {
        try_(|| {
            if self.next != UNKNOWN_KEY {
                self.element(self.next, value)?;
            }
            // see serialize_map_start
            self.next = UNKNOWN_KEY;
            Ok(())
        })
        .ctx(*self)
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
    fields: &'a [(ArrayBuilder, FieldMeta)],
    result: Option<usize>,
}

impl<'a> KeyLookupSerializer<'a> {
    pub fn lookup<K: Serialize + ?Sized>(
        fields: &'a [(ArrayBuilder, FieldMeta)],
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
        for (idx, (_, meta)) in self.fields.iter().enumerate() {
            if meta.name == v {
                self.result = Some(idx);
            }
        }
        Ok(())
    }
}
