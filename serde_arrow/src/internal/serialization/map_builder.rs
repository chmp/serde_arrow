use std::collections::BTreeMap;

use serde::Serialize;

use crate::internal::{
    arrow::{Array, FieldMeta, ListArray},
    error::{fail, set_default, Context, ContextSupport, Result},
    utils::array_ext::{ArrayExt, OffsetsArray, SeqArrayExt},
};

use super::{array_builder::ArrayBuilder, simple_serializer::SimpleSerializer};

#[derive(Debug, Clone)]
pub struct MapBuilder {
    pub path: String,
    pub meta: FieldMeta,
    pub entry: Box<ArrayBuilder>,
    pub offsets: OffsetsArray<i32>,
}

impl MapBuilder {
    pub fn new(
        path: String,
        meta: FieldMeta,
        entry: ArrayBuilder,
        is_nullable: bool,
    ) -> Result<Self> {
        Self::validate_entry(&entry)?;
        Ok(Self {
            path,
            meta,
            offsets: OffsetsArray::new(is_nullable),
            entry: Box::new(entry),
        })
    }

    fn validate_entry(entry: &ArrayBuilder) -> Result<()> {
        let ArrayBuilder::Struct(entry) = entry else {
            fail!("Entry field of a map must be a struct field");
        };
        if entry.fields.len() != 2 {
            fail!("Entry field of a map must be a struct field with 2 fields");
        }
        Ok(())
    }

    pub fn take(&mut self) -> ArrayBuilder {
        ArrayBuilder::Map(Self {
            path: self.path.clone(),
            meta: self.meta.clone(),
            offsets: self.offsets.take(),
            entry: Box::new(self.entry.take()),
        })
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

impl Context for MapBuilder {
    fn annotate(&self, annotations: &mut BTreeMap<String, String>) {
        set_default(annotations, "field", &self.path);
        set_default(annotations, "data_type", "Map(..)");
    }
}

impl SimpleSerializer for MapBuilder {
    fn serialize_default(&mut self) -> Result<()> {
        self.offsets.push_seq_default().ctx(self)
    }

    fn serialize_none(&mut self) -> Result<()> {
        self.offsets.push_seq_none().ctx(self)
    }

    fn serialize_map_start(&mut self, _: Option<usize>) -> Result<()> {
        self.offsets.start_seq().ctx(self)
    }

    fn serialize_map_key<V: Serialize + ?Sized>(&mut self, key: &V) -> Result<()> {
        self.offsets.push_seq_elements(1).ctx(self)?;
        self.entry.serialize_tuple_start(2).ctx(self)?;
        self.entry.serialize_tuple_element(key)
    }

    fn serialize_map_value<V: Serialize + ?Sized>(&mut self, value: &V) -> Result<()> {
        self.entry.serialize_tuple_element(value)?;
        self.entry.serialize_tuple_end().ctx(self)
    }

    fn serialize_map_end(&mut self) -> Result<()> {
        self.offsets.end_seq().ctx(self)
    }
}
