use std::collections::{BTreeMap, HashMap};

use marrow::{
    array::{Array, StructArray},
    datatypes::{Field, FieldMeta},
};
use serde::{Serialize, Serializer};

use crate::internal::{
    error::{fail, prepend, set_default, try_, Context, ContextSupport, Error, Result},
    serialization::{construction::build_struct, utils::impl_serializer},
    utils::array_ext::{ArrayExt, CountArray, SeqArrayExt},
};

use super::array_builder::ArrayBuilder;

const UNKNOWN_KEY: usize = usize::MAX;

#[derive(Debug, Clone)]
pub struct StructBuilder {
    pub name: String,
    pub fields: Vec<ArrayBuilder>,
    // Note: for the complex_1000 benchmark this optimization results in approx
    // 1.26 arrow2_convert times reduction
    lookup_cache: CachedNameLookup,
    pub next: usize,
    pub seen: Vec<bool>,
    pub seq: CountArray,
    pub metadata: HashMap<String, String>,
}

impl StructBuilder {
    pub fn from_fields(fields: Vec<Field>) -> Result<Self> {
        build_struct(String::from("$"), fields, false, Default::default())
    }

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
            lookup_cache: CachedNameLookup::new(fields.len()),
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
            lookup_cache: std::mem::replace(
                &mut self.lookup_cache,
                CachedNameLookup::new(self.fields.len()),
            ),
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

    pub fn serialize_value<V: Serialize>(&mut self, value: V) -> Result<()> {
        value.serialize(&mut *self).ctx(self)
    }

    pub fn num_fields(&self) -> usize {
        self.fields.len()
    }
}

impl StructBuilder {
    fn start(&mut self) -> Result<()> {
        self.seq.start_seq()?;
        self.seen.fill(false);
        self.next = 0;
        Ok(())
    }

    pub fn end(&mut self) -> Result<()> {
        self.seq.end_seq()?;
        for (seen, field) in std::iter::zip(&self.seen, &mut self.fields) {
            if !*seen {
                if !field.is_nullable() {
                    fail!(
                        "Missing non-nullable field {:?} in struct",
                        field.get_name(),
                    );
                }

                field.serialize_none()?;
            }
        }
        Ok(())
    }

    pub fn element<T: Serialize + ?Sized>(&mut self, idx: usize, value: &T) -> Result<()> {
        self.seq.push_seq_elements(1)?;
        if self.seen[idx] {
            fail!("Duplicate field {key:?}", key = self.fields[idx].get_name());
        }

        self.fields[idx].serialize_value(value)?;
        self.seen[idx] = true;
        self.next = idx + 1;
        Ok(())
    }
}

impl Context for StructBuilder {
    fn annotate(&self, annotations: &mut BTreeMap<String, String>) {
        prepend(annotations, "field", &self.name);
        set_default(annotations, "data_type", "Struct(..)");
    }
}

impl<'a> Serializer for &'a mut StructBuilder {
    impl_serializer!(
        'a, StructBuilder;
        override serialize_map,
        override serialize_none,
        override serialize_struct,
        override serialize_tuple,
    );

    fn serialize_none(self) -> Result<()> {
        self.seq.push_seq_none()?;
        for builder in &mut self.fields {
            builder.serialize_default_value()?;
        }
        Ok(())
    }

    fn serialize_struct(self, _name: &'static str, _len: usize) -> Result<Self::SerializeStruct> {
        self.start()?;
        Ok(Self::SerializeStruct::Struct(self))
    }

    fn serialize_map(self, _len: Option<usize>) -> Result<Self::SerializeMap> {
        self.start()?;
        // always re-set to an invalid field to force that `_key()` is called before `_value()`.
        self.next = UNKNOWN_KEY;
        Ok(Self::SerializeMap::Struct(self))
    }

    fn serialize_tuple(self, _len: usize) -> Result<Self::SerializeTuple> {
        self.start()?;
        Ok(Self::SerializeTuple::Struct(self))
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
        if let Some(idx) = self.lookup_cache.lookup(self.next, key, &self.fields) {
            self.element(idx, value)
        } else {
            // ignore unknown fields
            Ok(())
        }
    }

    fn end(self) -> Result<()> {
        StructBuilder::end(self)
    }
}

impl serde::ser::SerializeMap for &mut StructBuilder {
    type Ok = ();
    type Error = Error;

    fn serialize_key<T: ?Sized + Serialize>(&mut self, key: &T) -> Result<()> {
        self.next = KeyLookupSerializer::lookup(&self.fields, key)?.unwrap_or(UNKNOWN_KEY);
        Ok(())
    }

    fn serialize_value<T: ?Sized + Serialize>(&mut self, value: &T) -> Result<()> {
        if self.next != UNKNOWN_KEY {
            self.element(self.next, value)?;
        }
        self.next = UNKNOWN_KEY;
        Ok(())
    }

    fn end(self) -> Result<()> {
        StructBuilder::end(self)
    }
}

impl serde::ser::SerializeTuple for &mut StructBuilder {
    type Ok = ();
    type Error = Error;

    fn serialize_element<T: ?Sized + Serialize>(&mut self, value: &T) -> Result<()> {
        // ignore extra tuple fields
        if self.next < self.fields.len() {
            self.element(self.next, value)?;
        }
        Ok(())
    }

    fn end(self) -> Result<()> {
        StructBuilder::end(self)
    }
}

/// A wrapper around a static field name that compares using ptr and length
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct StaticFieldName(*const u8, usize);

unsafe impl Send for StaticFieldName {}

unsafe impl Sync for StaticFieldName {}

impl StaticFieldName {
    pub fn new(s: &'static str) -> Self {
        Self(s.as_ptr(), s.len())
    }
}

trait Named {
    fn get_name(&self) -> &str;
}

impl Named for ArrayBuilder {
    fn get_name(&self) -> &str {
        ArrayBuilder::get_name(self)
    }
}

impl Named for &str {
    fn get_name(&self) -> &str {
        self
    }
}

#[derive(Debug, Clone)]
struct CachedNameLookup {
    cache: Vec<Option<StaticFieldName>>,
}

impl CachedNameLookup {
    fn new(n_fields: usize) -> Self {
        Self {
            cache: vec![None; n_fields],
        }
    }

    fn lookup(&mut self, guess: usize, name: &'static str, fields: &[impl Named]) -> Option<usize> {
        if guess >= fields.len() || guess >= self.cache.len() {
            return self.lookup_field_loop(name, fields);
        }

        if self.cache[guess] == Some(StaticFieldName::new(name)) {
            Some(guess)
        } else if let Some(idx) = self.lookup_field_uncached(guess, name, fields) {
            if idx < self.cache.len() && self.cache[idx].is_none() {
                self.cache[idx] = Some(StaticFieldName::new(name));
            }
            Some(idx)
        } else {
            None
        }
    }

    pub fn lookup_field_uncached(
        &self,
        guess: usize,
        name: &str,
        fields: &[impl Named],
    ) -> Option<usize> {
        if fields[guess].get_name() == name {
            Some(guess)
        } else {
            self.lookup_field_loop(name, fields)
        }
    }

    fn lookup_field_loop(&self, name: &str, fields: &[impl Named]) -> Option<usize> {
        fields.iter().position(|field| field.get_name() == name)
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

#[test]
fn example() {
    let mut lookup = CachedNameLookup::new(3);

    const FOO: &str = "foo";
    const BAR: &str = "bar";
    const BAZ: &str = "baz";

    assert_eq!(lookup.lookup(0, FOO, &["foo", "bar", "baz"]), Some(0));
    assert_eq!(lookup.lookup(1, BAR, &["foo", "bar", "baz"]), Some(1));
    assert_eq!(lookup.lookup(2, BAZ, &["foo", "bar", "baz"]), Some(2));

    assert!(lookup.cache[0].is_some());
    assert_eq!(lookup.cache[0], Some(StaticFieldName::new(FOO)));

    assert!(lookup.cache[1].is_some());
    assert_eq!(lookup.cache[1], Some(StaticFieldName::new(BAR)));

    assert!(lookup.cache[2].is_some());
    assert_eq!(lookup.cache[2], Some(StaticFieldName::new(BAZ)));

    assert_eq!(lookup.lookup(0, FOO, &["foo", "bar", "baz"]), Some(0));
    assert_eq!(lookup.lookup(1, BAR, &["foo", "bar", "baz"]), Some(1));
    assert_eq!(lookup.lookup(2, BAZ, &["foo", "bar", "baz"]), Some(2));

    assert_eq!(lookup.lookup(0, FOO, &["foo", "bar", "baz"]), Some(0));
    assert_eq!(lookup.lookup(1, FOO, &["foo", "bar", "baz"]), Some(0));
    assert_eq!(lookup.lookup(2, FOO, &["foo", "bar", "baz"]), Some(0));

    assert_eq!(lookup.lookup(0, BAR, &["foo", "bar", "baz"]), Some(1));
    assert_eq!(lookup.lookup(1, BAR, &["foo", "bar", "baz"]), Some(1));
    assert_eq!(lookup.lookup(2, BAR, &["foo", "bar", "baz"]), Some(1));

    assert_eq!(lookup.lookup(0, BAZ, &["foo", "bar", "baz"]), Some(2));
    assert_eq!(lookup.lookup(1, BAZ, &["foo", "bar", "baz"]), Some(2));
    assert_eq!(lookup.lookup(2, BAZ, &["foo", "bar", "baz"]), Some(2));
}
