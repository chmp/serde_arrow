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

const UNKNOWN_KEY: usize = usize::MAX;

#[derive(Debug, Clone)]
pub struct StructBuilder {
    pub fields: Vec<GenericField>,
    pub validity: Option<MutableBitBuffer>,
    pub named_fields: Vec<(String, ArrayBuilder)>,
    pub cached_names: Vec<Option<(*const u8, usize)>>,
    pub seen: Vec<bool>,
    pub next: usize,
    pub index: BTreeMap<String, usize>,
    pub key_serializer: KeySerializer,
}

impl StructBuilder {
    pub fn new(
        fields: Vec<GenericField>,
        named_fields: Vec<(String, ArrayBuilder)>,
        is_nullable: bool,
    ) -> Result<Self> {
        let mut index = BTreeMap::new();
        let cached_names = vec![None; named_fields.len()];
        let seen = vec![false; named_fields.len()];
        let next = 0;

        if fields.len() != named_fields.len() {
            fail!("mismatched number of fields and builders");
        }

        let mut capacity = 0;
        for (idx, (name, _)) in named_fields.iter().enumerate() {
            if index.contains_key(name) {
                fail!("Duplicate field {name}");
            }
            index.insert(name.to_owned(), idx);
            capacity = std::cmp::max(capacity, name.len());
        }

        let key_serializer = KeySerializer::with_capacity(capacity);

        Ok(Self {
            fields,
            validity: is_nullable.then(MutableBitBuffer::default),
            named_fields,
            cached_names,
            seen,
            next,
            index,
            key_serializer,
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
            cached_names: std::mem::replace(&mut self.cached_names, vec![None; self.named_fields.len()]),
            seen: std::mem::replace(&mut self.seen, vec![false; self.named_fields.len()]),
            next: std::mem::take(&mut self.next),
            index: self.index.clone(),
            key_serializer: self.key_serializer.clone(),
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
        for seen in &mut self.seen {
            *seen = false;
        }
        self.next = 0;
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
        self.next = idx + 1;
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
        let fast_key = (key.as_ptr(), key.len());
        let idx = if self.next < self.cached_names.len()
            && Some(fast_key) == self.cached_names[self.next]
        {
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
        key.serialize(Mut(&mut self.key_serializer))?;
        self.next = self
            .index
            .get(&self.key_serializer.0)
            .cloned()
            .unwrap_or(UNKNOWN_KEY);
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
pub struct KeySerializer(String);

impl KeySerializer {
    fn with_capacity(capacity: usize) -> Self {
        Self(String::with_capacity(capacity))
    }
}

impl std::clone::Clone for KeySerializer {
    fn clone(&self) -> Self {
        Self(String::with_capacity(self.0.capacity()))
    }
}

impl SimpleSerializer for KeySerializer {
    fn name(&self) -> &str {
        "KeySerializer"
    }

    fn serialize_str(&mut self, v: &str) -> Result<()> {
        self.0.clear();
        self.0.push_str(v);
        Ok(())
    }
}
