use serde::Serialize;

use crate::internal::{
    arrow::{Array, ListArray},
    error::Result,
    schema::GenericField,
};

use super::{
    array_builder::ArrayBuilder,
    utils::{
        meta_from_field, push_validity, push_validity_default, MutableBitBuffer,
        MutableOffsetBuffer, SimpleSerializer,
    },
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

    pub fn into_array(self) -> Result<Array> {
        Ok(Array::Map(ListArray {
            meta: meta_from_field(self.entry_field)?,
            element: Box::new((*self.entry).into_array()?),
            validity: self.validity.map(|v| v.buffer),
            offsets: self.offsets.offsets,
        }))
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

    fn serialize_map_start(&mut self, _: Option<usize>) -> Result<()> {
        push_validity(&mut self.validity, true)?;
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
