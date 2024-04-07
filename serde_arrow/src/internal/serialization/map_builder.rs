use serde::Serialize;

use crate::{
    internal::{
        common::{MutableBitBuffer, MutableOffsetBuffer},
        schema::GenericField,
    },
    Result,
};

use super::{
    array_builder::ArrayBuilder,
    utils::{push_validity, push_validity_default, SimpleSerializer},
};

#[derive(Debug, Clone)]
pub struct MapBuilder {
    pub entry_field: GenericField,
    pub validity: Option<MutableBitBuffer>,
    pub offsets: MutableOffsetBuffer<i32>,
    pub entry: Box<ArrayBuilder>,
}

impl MapBuilder {
    pub fn new(entry_field: GenericField, entry: ArrayBuilder, is_nullable: bool) -> Self {
        Self {
            entry_field,
            validity: is_nullable.then(MutableBitBuffer::default),
            offsets: MutableOffsetBuffer::default(),
            entry: Box::new(entry),
        }
    }

    pub fn take(&mut self) -> Self {
        Self {
            entry_field: self.entry_field.clone(),
            validity: self.validity.as_mut().map(std::mem::take),
            offsets: std::mem::take(&mut self.offsets),
            entry: Box::new(self.entry.take()),
        }
    }

    pub fn is_nullable(&self) -> bool {
        self.validity.is_some()
    }

    pub fn reserve(&mut self, num_elements: usize) -> Result<()> {
        if let Some(validity) = self.validity.as_mut() {
            validity.reserve(num_elements);
        }
        self.offsets.reserve(num_elements);
        Ok(())
    }
}

impl SimpleSerializer for MapBuilder {
    fn name(&self) -> &str {
        "MapBuilder"
    }

    fn serialize_default(&mut self) -> Result<()> {
        push_validity_default(&mut self.validity);
        self.offsets.push_current_items();
        Ok(())
    }

    fn serialize_none(&mut self) -> Result<()> {
        self.offsets.push_current_items();
        push_validity(&mut self.validity, false)
    }

    fn serialize_map_start(&mut self, num_elements: Option<usize>) -> Result<()> {
        push_validity(&mut self.validity, true)?;
        if let Some(num_elements) = num_elements {
            self.entry.reserve(num_elements)?;
        }
        Ok(())
    }

    fn serialize_map_key<V: Serialize + ?Sized>(&mut self, key: &V) -> Result<()> {
        self.offsets.inc_current_items()?;
        self.entry.serialize_tuple_start(2)?;
        self.entry.serialize_tuple_element(key)
    }

    fn serialize_map_value<V: Serialize + ?Sized>(&mut self, value: &V) -> Result<()> {
        self.entry.serialize_tuple_element(value)?;
        self.entry.serialize_tuple_end()
    }

    fn serialize_map_end(&mut self) -> Result<()> {
        self.offsets.push_current_items();
        Ok(())
    }
}
