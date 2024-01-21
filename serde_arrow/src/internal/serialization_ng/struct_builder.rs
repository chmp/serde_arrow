use std::collections::BTreeMap;

use serde::Serialize;

use crate::{
    internal::{common::MutableBitBuffer, error::fail},
    Result,
};

use super::{
    array_builder::ArrayBuilder,
    utils::{push_validity, push_validity_default, Mut, SimpleSerializer},
};

#[derive(Debug, Clone)]
pub struct StructBuilder {
    pub validity: Option<MutableBitBuffer>,
    pub named_fields: Vec<(String, ArrayBuilder)>,
    pub cached_names: Vec<Option<(*const u8, usize)>>,
    pub seen: Vec<bool>,
    pub next: usize,
    pub index: BTreeMap<String, usize>,
}

impl StructBuilder {
    pub fn new(named_fields: Vec<(String, ArrayBuilder)>, is_nullable: bool) -> Result<Self> {
        let mut index = BTreeMap::new();
        let cached_names = vec![None; named_fields.len()];
        let seen = vec![false; named_fields.len()];
        let next = 0;

        for (idx, (name, _)) in named_fields.iter().enumerate() {
            if index.contains_key(name) {
                fail!("Duplicate field {name}");
            }
            index.insert(name.to_owned(), idx);
        }

        Ok(Self {
            validity: is_nullable.then(MutableBitBuffer::default),
            named_fields,
            cached_names,
            seen,
            next,
            index,
        })
    }

    pub fn take(&mut self) -> Self {
        Self {
            validity: self.validity.as_mut().map(std::mem::take),
            named_fields: self
                .named_fields
                .iter_mut()
                .map(|(name, builder)| (name.clone(), builder.take()))
                .collect(),
            cached_names: vec![None; self.cached_names.len()],
            seen: vec![false; self.seen.len()],
            next: 0,
            index: self.index.clone(),
        }
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

    fn serialize_some<V: Serialize + ?Sized>(&mut self, value: &V) -> Result<()> {
        value.serialize(Mut(self))
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
        self.element(self.next, value)
    }

    fn serialize_tuple_struct_end(&mut self) -> Result<()> {
        self.end()
    }
}