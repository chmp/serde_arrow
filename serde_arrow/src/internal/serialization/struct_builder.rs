use std::collections::BTreeMap;

use serde::Serialize;

use crate::internal::{
    arrow::{Array, StructArray},
    error::{fail, Result},
    schema::GenericField,
    utils::Mut,
};

use super::{
    array_builder::ArrayBuilder,
    array_ext::{ArrayExt, CountArray, SeqArrayExt},
    utils::{meta_from_field, SimpleSerializer},
};

const UNKNOWN_KEY: usize = usize::MAX;

#[derive(Debug, Clone)]
pub struct StructBuilder {
    // TODO: clean this up
    pub fields: Vec<GenericField>,
    pub named_fields: Vec<(String, ArrayBuilder)>,
    pub cached_names: Vec<Option<(*const u8, usize)>>,
    pub seen: Vec<bool>,
    pub next: usize,
    pub index: BTreeMap<String, usize>,
    pub seq: CountArray,
}

impl StructBuilder {
    pub fn new(
        fields: Vec<GenericField>,
        named_fields: Vec<(String, ArrayBuilder)>,
        is_nullable: bool,
    ) -> Result<Self> {
        if fields.len() != named_fields.len() {
            fail!("mismatched number of fields and builders");
        }

        let mut index = BTreeMap::new();
        for (idx, (name, _)) in named_fields.iter().enumerate() {
            if index.contains_key(name) {
                fail!("Duplicate field {name}");
            }
            index.insert(name.to_owned(), idx);
        }

        Ok(Self {
            fields,
            seen: vec![false; named_fields.len()],
            cached_names: vec![None; named_fields.len()],
            named_fields,
            next: 0,
            index,
            seq: CountArray::new(is_nullable),
        })
    }

    pub fn take(&mut self) -> Self {
        Self {
            fields: self.fields.clone(),
            named_fields: self
                .named_fields
                .iter_mut()
                .map(|(name, builder)| (name.clone(), builder.take()))
                .collect(),
            cached_names: std::mem::replace(
                &mut self.cached_names,
                vec![None; self.named_fields.len()],
            ),
            seen: std::mem::replace(&mut self.seen, vec![false; self.named_fields.len()]),
            next: std::mem::take(&mut self.next),
            index: self.index.clone(),
            seq: self.seq.take(),
        }
    }

    pub fn is_nullable(&self) -> bool {
        self.seq.validity.is_some()
    }

    pub fn into_array(self) -> Result<Array> {
        let mut fields = Vec::new();
        for (field, (_, builder)) in self.fields.into_iter().zip(self.named_fields) {
            let meta = meta_from_field(field)?;
            let array = builder.into_array()?;
            fields.push((array, meta));
        }

        Ok(Array::Struct(StructArray {
            len: self.seq.len,
            validity: self.seq.validity,
            fields,
        }))
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
        self.seq.push_seq_elements(1)?;
        if self.seen[idx] {
            fail!("Duplicate field {key}", key = self.named_fields[idx].0);
        }

        value.serialize(Mut(&mut self.named_fields[idx].1))?;
        self.seen[idx] = true;
        self.next = idx + 1;
        Ok(())
    }
}

impl SimpleSerializer for StructBuilder {
    fn name(&self) -> &str {
        "StructBuilder"
    }

    fn serialize_default(&mut self) -> Result<()> {
        self.seq.push_seq_default()?;
        for (_, field) in &mut self.named_fields {
            field.serialize_default()?;
        }

        Ok(())
    }

    fn serialize_none(&mut self) -> Result<()> {
        self.seq.push_seq_none()?;
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
        let fast_key = (key.as_ptr(), key.len());
        let idx = if self.cached_names.get(self.next) == Some(&Some(fast_key)) {
            self.next
        } else {
            let Some(&idx) = self.index.get(key) else {
                // ignore unknown fields
                return Ok(());
            };

            if self.cached_names[idx].is_none() {
                self.cached_names[idx] = Some(fast_key);
            }
            idx
        };

        self.element(idx, value)
    }

    fn serialize_struct_end(&mut self) -> Result<()> {
        self.end()
    }

    fn serialize_tuple_start(&mut self, _: usize) -> Result<()> {
        self.start()
    }

    fn serialize_tuple_element<V: Serialize + ?Sized>(&mut self, value: &V) -> Result<()> {
        self.element(self.next, value)
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
        // always re-set to an invalid field to force that `_key()` is called before `_value()`.
        self.next = UNKNOWN_KEY;
        Ok(())
    }

    fn serialize_map_key<V: Serialize + ?Sized>(&mut self, key: &V) -> Result<()> {
        self.next = KeyLookupSerializer::lookup(&self.index, key)?.unwrap_or(UNKNOWN_KEY);
        Ok(())
    }

    fn serialize_map_value<V: Serialize + ?Sized>(&mut self, value: &V) -> Result<()> {
        if self.next != UNKNOWN_KEY {
            self.element(self.next, value)?;
        }
        // see serialize_map_start
        self.next = UNKNOWN_KEY;
        Ok(())
    }

    fn serialize_map_end(&mut self) -> Result<()> {
        self.end()
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

impl<'a> SimpleSerializer for KeyLookupSerializer<'a> {
    fn name(&self) -> &str {
        "KeyLookupSerializer"
    }

    fn serialize_str(&mut self, v: &str) -> Result<()> {
        self.result = self.index.get(v).copied();
        Ok(())
    }
}
