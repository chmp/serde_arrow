use std::collections::BTreeMap;

use serde::Serialize;

use crate::internal::{
    arrow::{Array, FieldMeta, StructArray},
    error::{fail, Result},
    utils::Mut,
};

use super::{
    array_builder::ArrayBuilder,
    array_ext::{ArrayExt, CountArray, SeqArrayExt},
    utils::SimpleSerializer,
};

const UNKNOWN_KEY: usize = usize::MAX;

#[derive(Debug, Clone)]
pub struct StructBuilder {
    pub fields: Vec<(ArrayBuilder, FieldMeta)>,
    pub cached_names: Vec<Option<(*const u8, usize)>>,
    pub seen: Vec<bool>,
    pub next: usize,
    pub index: BTreeMap<String, usize>,
    pub seq: CountArray,
}

impl StructBuilder {
    pub fn new(fields: Vec<(ArrayBuilder, FieldMeta)>, is_nullable: bool) -> Result<Self> {
        let mut index = BTreeMap::new();
        for (idx, (_, meta)) in fields.iter().enumerate() {
            if index.contains_key(&meta.name) {
                fail!("Duplicate field {name}", name = meta.name);
            }
            index.insert(meta.name.clone(), idx);
        }

        Ok(Self {
            seen: vec![false; fields.len()],
            cached_names: vec![None; fields.len()],
            fields,
            next: 0,
            index,
            seq: CountArray::new(is_nullable),
        })
    }

    pub fn take(&mut self) -> Self {
        Self {
            fields: self
                .fields
                .iter_mut()
                .map(|(builder, meta)| (builder.take(), meta.clone()))
                .collect(),
            cached_names: std::mem::replace(&mut self.cached_names, vec![None; self.fields.len()]),
            seen: std::mem::replace(&mut self.seen, vec![false; self.fields.len()]),
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
        for (builder, meta) in self.fields {
            fields.push((builder.into_array()?, meta));
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
                if !self.fields[idx].1.nullable {
                    fail!(
                        "missing non-nullable field {:?} in struct",
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
            fail!("Duplicate field {key}", key = self.fields[idx].1.name);
        }

        value.serialize(Mut(&mut self.fields[idx].0))?;
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
        for (builder, _) in &mut self.fields {
            builder.serialize_default()?;
        }

        Ok(())
    }

    fn serialize_none(&mut self) -> Result<()> {
        self.seq.push_seq_none()?;
        for (builder, _) in &mut self.fields {
            builder.serialize_default()?;
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
        if self.next < self.fields.len() {
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
