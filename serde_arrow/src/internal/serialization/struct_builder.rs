use std::collections::BTreeMap;

use marrow::{
    array::{Array, StructArray},
    datatypes::FieldMeta,
};
use serde::Serialize;

use crate::internal::{
    error::{fail, set_default, try_, Context, ContextSupport, Result},
    utils::{
        array_ext::{ArrayExt, CountArray, SeqArrayExt},
        Mut,
    },
};

use super::{array_builder::ArrayBuilder, simple_serializer::SimpleSerializer};

const UNKNOWN_KEY: usize = usize::MAX;

#[derive(Debug, Clone)]
pub struct StructBuilder {
    pub path: String,
    pub fields: Vec<(ArrayBuilder, FieldMeta)>,
    pub lookup: FieldLookup,
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
        let lookup = FieldLookup::new(fields.iter().map(|(_, meta)| meta.name.clone()).collect())?;

        Ok(Self {
            path,
            seq: CountArray::new(is_nullable),
            seen: vec![false; fields.len()],
            next: 0,
            lookup,
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
            lookup: self.lookup.take(),
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

    fn end(&mut self) -> Result<()> {
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

    fn element<T: Serialize + ?Sized>(&mut self, idx: usize, value: &T) -> Result<()> {
        self.seq.push_seq_elements(1)?;
        if self.seen[idx] {
            fail!(in self, "Duplicate field {key}", key = self.fields[idx].1.name);
        }

        value.serialize(Mut(&mut self.fields[idx].0))?;
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

impl SimpleSerializer for StructBuilder {
    fn serialize_default(&mut self) -> Result<()> {
        try_(|| {
            self.seq.push_seq_default()?;
            for (builder, _) in &mut self.fields {
                builder.serialize_default()?;
            }

            Ok(())
        })
        .ctx(self)
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
            let Some(idx) = self.lookup.lookup(self.next, key) else {
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
            self.next = self.lookup.lookup_serialize(key)?.unwrap_or(UNKNOWN_KEY);
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

/// Optimize field lookups for static names
#[derive(Debug, Clone)]
pub struct FieldLookup {
    pub cached_names: Vec<Option<StaticFieldName>>,
    pub index: BTreeMap<String, usize>,
}

/// A wrapper around a static field name that compares using ptr and length
#[derive(Debug, Clone)]
pub struct StaticFieldName(&'static str);

impl std::cmp::PartialEq for StaticFieldName {
    fn eq(&self, other: &Self) -> bool {
        (self.0.as_ptr(), self.0.len()) == (other.0.as_ptr(), other.0.len())
    }
}

impl FieldLookup {
    pub fn new(field_names: Vec<String>) -> Result<Self> {
        let mut index = BTreeMap::new();
        for (idx, name) in field_names.into_iter().enumerate() {
            if index.contains_key(&name) {
                fail!("Duplicate field {name}");
            }
            index.insert(name, idx);
        }
        Ok(Self {
            cached_names: vec![None; index.len()],
            index,
        })
    }

    pub fn take(&mut self) -> Self {
        Self {
            cached_names: std::mem::replace(&mut self.cached_names, vec![None; self.index.len()]),
            index: self.index.clone(),
        }
    }

    pub fn lookup(&mut self, guess: usize, key: &'static str) -> Option<usize> {
        if self.cached_names.get(guess) == Some(&Some(StaticFieldName(key))) {
            Some(guess)
        } else {
            let &idx = self.index.get(key)?;
            if self.cached_names[idx].is_none() {
                self.cached_names[idx] = Some(StaticFieldName(key));
            }
            Some(idx)
        }
    }

    pub fn lookup_serialize<V: Serialize + ?Sized>(&mut self, key: &V) -> Result<Option<usize>> {
        KeyLookupSerializer::lookup(&self.index, key)
    }
}

#[derive(Debug)]
pub struct KeyLookupSerializer<'a> {
    index: &'a BTreeMap<String, usize>,
    result: Option<usize>,
}

impl<'a> KeyLookupSerializer<'a> {
    pub fn lookup<K: Serialize + ?Sized>(
        index: &'a BTreeMap<String, usize>,
        key: &K,
    ) -> Result<Option<usize>> {
        let mut this = Self {
            index,
            result: None,
        };
        key.serialize(Mut(&mut this))?;
        Ok(this.result)
    }
}

impl Context for KeyLookupSerializer<'_> {
    fn annotate(&self, _: &mut BTreeMap<String, String>) {}
}

impl SimpleSerializer for KeyLookupSerializer<'_> {
    fn serialize_str(&mut self, v: &str) -> Result<()> {
        self.result = self.index.get(v).copied();
        Ok(())
    }
}
