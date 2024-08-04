use serde::Serialize;

use crate::internal::{
    arrow::{Array, FieldMeta, ListArray},
    error::Result,
};

use super::{
    array_builder::ArrayBuilder,
    array_ext::{ArrayExt, OffsetsArray, SeqArrayExt},
    simple_serializer::SimpleSerializer,
};

#[derive(Debug, Clone)]
pub struct MapBuilder {
    pub meta: FieldMeta,
    pub entry: Box<ArrayBuilder>,
    pub offsets: OffsetsArray<i32>,
}

impl MapBuilder {
    pub fn new(meta: FieldMeta, entry: ArrayBuilder, is_nullable: bool) -> Self {
        Self {
            meta,
            offsets: OffsetsArray::new(is_nullable),
            entry: Box::new(entry),
        }
    }

    pub fn take(&mut self) -> Self {
        Self {
            meta: self.meta.clone(),
            offsets: self.offsets.take(),
            entry: Box::new(self.entry.take()),
        }
    }

    pub fn is_nullable(&self) -> bool {
        self.offsets.validity.is_some()
    }

    pub fn into_array(self) -> Result<Array> {
        Ok(Array::Map(ListArray {
            meta: self.meta,
            element: Box::new((*self.entry).into_array()?),
            validity: self.offsets.validity,
            offsets: self.offsets.offsets,
        }))
    }
}

impl SimpleSerializer for MapBuilder {
    fn name(&self) -> &str {
        "MapBuilder"
    }

    fn serialize_default(&mut self) -> Result<()> {
        self.offsets.push_seq_default()
    }

    fn serialize_none(&mut self) -> Result<()> {
        self.offsets.push_seq_none()
    }

    fn serialize_map_start(&mut self, _: Option<usize>) -> Result<()> {
        self.offsets.start_seq()
    }

    fn serialize_map_key<V: Serialize + ?Sized>(&mut self, key: &V) -> Result<()> {
        self.offsets.push_seq_elements(1)?;
        self.entry.serialize_tuple_start(2)?;
        self.entry.serialize_tuple_element(key)
    }

    fn serialize_map_value<V: Serialize + ?Sized>(&mut self, value: &V) -> Result<()> {
        self.entry.serialize_tuple_element(value)?;
        self.entry.serialize_tuple_end()
    }

    fn serialize_map_end(&mut self) -> Result<()> {
        self.offsets.end_seq()
    }
}
