use std::collections::BTreeMap;

use serde::Serialize;

use crate::{
    internal::{common::MutableBitBuffer, error::fail, schema::GenericField},
    Result,
};

use super::{
    array_builder::ArrayBuilder,
    utils::{push_validity, push_validity_default, Mut, SimpleSerializer},
};

#[derive(Debug, Clone)]
pub struct StructBuilder {
    pub fields: Vec<GenericField>,
    pub validity: Option<MutableBitBuffer>,
    pub named_fields: Vec<(String, ArrayBuilder)>,
    pub seen: Vec<bool>,
    pub next: usize,
    pub key_idx: Option<usize>,
    pub struct_index: StructKeyIndex,
    pub map_index: MapKeyIndex,
}

impl StructBuilder {
    pub fn new(
        fields: Vec<GenericField>,
        named_fields: Vec<(String, ArrayBuilder)>,
        is_nullable: bool,
    ) -> Result<Self> {
        let mut index = BTreeMap::new();
        let seen = vec![false; named_fields.len()];
        let next = 0;

        if fields.len() != named_fields.len() {
            fail!("mismatched number of fields and builders");
        }

        for (idx, (name, _)) in named_fields.iter().enumerate() {
            if index.contains_key(name) {
                fail!("Duplicate field {name}");
            }
            index.insert(name.to_owned(), idx);
        }

        let struct_index = StructKeyIndex::new(index.clone());
        let map_index = MapKeyIndex::new(index)?;

        Ok(Self {
            fields,
            validity: is_nullable.then(MutableBitBuffer::default),
            named_fields,
            seen,
            next,
            struct_index,
            map_index,
            key_idx: None,
        })
    }

    pub fn take(&mut self) -> Self {
        Self {
            fields: self.fields.clone(),
            validity: self.validity.as_mut().map(std::mem::take),
            named_fields: self
                .named_fields
                .iter_mut()
                .map(|(name, builder)| (name.clone(), builder.take()))
                .collect(),
            seen: std::mem::replace(&mut self.seen, vec![false; self.named_fields.len()]),
            next: std::mem::take(&mut self.next),
            struct_index: self.struct_index.take(),
            map_index: self.map_index.take(),
            key_idx: None,
        }
    }

    pub fn is_nullable(&self) -> bool {
        self.validity.is_some()
    }
}

impl StructBuilder {
    fn start(&mut self) -> Result<()> {
        push_validity(&mut self.validity, true)?;
        self.reset();
        Ok(())
    }

    fn reset(&mut self) {
        self.seen.fill(false);
        self.next = 0;
        self.key_idx = None;
    }

    fn end(&mut self) -> Result<()> {
        for (idx, seen) in self.seen.iter_mut().enumerate() {
            if !*seen {
                if !self.named_fields[idx].1.is_nullable() {
                    fail!(
                        "missing non-nullable field {:?} in struct",
                        self.named_fields[idx].0
                    );
                }

                self.named_fields[idx].1.serialize_none()?;
            }
        }
        Ok(())
    }

    fn element<T: Serialize + ?Sized>(&mut self, idx: usize, value: &T) -> Result<()> {
        if self.seen[idx] {
            fail!("Duplicate field {key}", key = self.named_fields[idx].0);
        }

        value.serialize(Mut(&mut self.named_fields[idx].1))?;
        self.seen[idx] = true;
        self.next += 1;
        Ok(())
    }
}

impl SimpleSerializer for StructBuilder {
    fn name(&self) -> &str {
        "StructBuilder"
    }

    fn serialize_default(&mut self) -> Result<()> {
        push_validity_default(&mut self.validity);
        for (_, field) in &mut self.named_fields {
            field.serialize_default()?;
        }

        Ok(())
    }

    fn serialize_none(&mut self) -> Result<()> {
        push_validity(&mut self.validity, false)?;

        for (_, field) in &mut self.named_fields {
            field.serialize_default()?;
        }

        Ok(())
    }

    fn serialize_struct_start(&mut self, _: &'static str, _: usize) -> Result<()> {
        self.start()
    }

    fn serialize_struct_field<T: Serialize + ?Sized>(
        &mut self,
        key: &'static str,
        value: &T,
    ) -> Result<()> {
        // NOTE: ignore unknown fields
        if let Some(idx) = self.struct_index.get(self.next, key) {
            self.element(idx, value)?;
        }
        Ok(())
    }

    fn serialize_struct_end(&mut self) -> Result<()> {
        self.end()
    }

    fn serialize_tuple_start(&mut self, _: usize) -> Result<()> {
        self.start()
    }

    fn serialize_tuple_element<V: Serialize + ?Sized>(&mut self, value: &V) -> Result<()> {
        // ignore extra tuple fields
        if self.next < self.named_fields.len() {
            self.element(self.next, value)?;
        }
        Ok(())
    }

    fn serialize_tuple_end(&mut self) -> Result<()> {
        self.end()
    }

    fn serialize_tuple_struct_start(&mut self, _: &'static str, _: usize) -> Result<()> {
        self.start()
    }

    fn serialize_tuple_struct_field<V: Serialize + ?Sized>(&mut self, value: &V) -> Result<()> {
        // ignore extra tuple fields
        if self.next < self.named_fields.len() {
            self.element(self.next, value)?;
        }
        Ok(())
    }

    fn serialize_tuple_struct_end(&mut self) -> Result<()> {
        self.end()
    }

    fn serialize_map_start(&mut self, _: Option<usize>) -> Result<()> {
        self.start()?;
        Ok(())
    }

    fn serialize_map_key<V: Serialize + ?Sized>(&mut self, key: &V) -> Result<()> {
        self.key_idx = KeyLookupSerializer::lookup(&mut self.map_index, self.next, key)?;
        Ok(())
    }

    fn serialize_map_value<V: Serialize + ?Sized>(&mut self, value: &V) -> Result<()> {
        if let Some(idx) = self.key_idx {
            self.element(idx, value)?;
        } else {
            self.next += 1;
        }
        Ok(())
    }

    fn serialize_map_end(&mut self) -> Result<()> {
        self.end()
    }
}

#[derive(Debug)]
pub struct KeyLookupSerializer<'a> {
    index: &'a mut MapKeyIndex,
    guess: usize,
    result: Option<usize>,
}

impl<'a> KeyLookupSerializer<'a> {
    pub fn lookup<K: Serialize + ?Sized>(
        index: &'a mut MapKeyIndex,
        guess: usize,
        key: &K,
    ) -> Result<Option<usize>> {
        let mut this = Self {
            index,
            guess,
            result: None,
        };
        key.serialize(Mut(&mut this))?;
        Ok(this.result)
    }
}

impl<'a> SimpleSerializer for KeyLookupSerializer<'a> {
    fn name(&self) -> &str {
        "KeyLookupSerializer"
    }

    fn serialize_str(&mut self, key: &str) -> Result<()> {
        self.result = self.index.get(self.guess, key);
        Ok(())
    }
}

#[derive(Debug, Clone)]
pub struct StructKeyIndex {
    pub cached_lookup: Vec<Option<CachedLookup>>,
    pub index: BTreeMap<String, usize>,
}

#[derive(Debug, Clone)]
pub struct CachedLookup {
    ptr: *const u8,
    len: usize,
    field_idx: usize,
}

impl CachedLookup {
    pub fn matches_ptr(&self, s: &'static str) -> bool {
        (self.ptr, self.len) == (s.as_ptr(), s.len())
    }
}

impl StructKeyIndex {
    fn new(index: BTreeMap<String, usize>) -> Self {
        Self {
            cached_lookup: vec![None; index.len()],
            index,
        }
    }

    fn take(&mut self) -> Self {
        Self {
            cached_lookup: std::mem::replace(&mut self.cached_lookup, vec![None; self.index.len()]),
            index: self.index.clone(),
        }
    }

    fn get(&mut self, guess: usize, key: &'static str) -> Option<usize> {
        if let Some(idx) = self.guess_matches(guess, key) {
            Some(idx)
        } else {
            // ignore unknown fields
            let idx = *(self.index.get(key)?);

            if guess < self.cached_lookup.len() && self.cached_lookup[guess].is_none() {
                self.cached_lookup[guess] = Some(CachedLookup {
                    ptr: key.as_ptr(),
                    len: key.len(),
                    field_idx: idx,
                });
            }
            Some(idx)
        }
    }

    fn guess_matches(&self, guess: usize, key: &'static str) -> Option<usize> {
        let cn = self.cached_lookup.get(guess)?.as_ref()?;
        if cn.matches_ptr(key) {
            Some(cn.field_idx)
        } else {
            None
        }
    }
}

#[test]
fn struct_key_index() {
    let index = BTreeMap::from([
        (String::from("foo"), 0),
        (String::from("bar"), 1),
        (String::from("baz"), 2),
    ]);

    let mut cached_index = StructKeyIndex::new(index.clone());

    let order0 = ["bar", "foo", "baz"];
    for (guess, key) in order0.into_iter().enumerate() {
        assert_eq!(cached_index.get(guess, key), index.get(key).copied());
    }

    // test that the keys are correctly resolved, even if the order changes
    let order1 = ["foo", "bar", "baz"];
    for (guess, key) in order1.into_iter().enumerate() {
        assert_eq!(cached_index.get(guess, key), index.get(key).copied());
    }

    let order2 = ["baz", "bar", "foo"];
    for (guess, key) in order2.into_iter().enumerate() {
        assert_eq!(cached_index.get(guess, key), index.get(key).copied());
    }

    assert_eq!(cached_index.get(0, "hello"), None);

    // test the internal state
    for (guess, key) in order0.into_iter().enumerate() {
        assert_eq!(
            cached_index.cached_lookup[guess]
                .as_ref()
                .unwrap()
                .field_idx,
            index.get(key).copied().unwrap()
        );
        assert_eq!(
            cached_index.cached_lookup[guess]
                .as_ref()
                .unwrap()
                .matches_ptr(key),
            true
        );
    }

    drop(index);
}

#[derive(Debug, Clone)]
pub struct MapKeyIndex {
    pub cached_lookup: Vec<Option<usize>>,
    pub index: BTreeMap<String, usize>,
    pub names: Vec<String>,
}

impl MapKeyIndex {
    pub fn new(index: BTreeMap<String, usize>) -> Result<Self> {
        let mut names = vec![None; index.len()];
        for (key, idx) in index.iter() {
            names[*idx] = Some(key.to_owned());
        }
        let Some(names) = names.into_iter().collect::<Option<Vec<_>>>() else {
            fail!("non sequential names");
        };

        Ok(Self {
            cached_lookup: vec![None; index.len()],
            names,
            index,
        })
    }

    pub fn take(&mut self) -> Self {
        Self {
            cached_lookup: vec![None; self.index.len()],
            index: self.index.clone(),
            names: self.names.clone(),
        }
    }

    pub fn get(&mut self, guess: usize, key: &str) -> Option<usize> {
        if let Some(idx) = self.get_cached(guess, key) {
            Some(idx)
        } else {
            let idx = self.index.get(key).copied()?;
            if guess < self.cached_lookup.len() && self.cached_lookup[guess].is_none() {
                self.cached_lookup[guess] = Some(idx);
            }
            Some(idx)
        }
    }

    pub fn get_cached(&self, guess: usize, key: &str) -> Option<usize> {
        let idx = self.cached_lookup.get(guess).copied()??;
        if self.names[idx] == key {
            Some(idx)
        } else {
            None
        }
    }
}
