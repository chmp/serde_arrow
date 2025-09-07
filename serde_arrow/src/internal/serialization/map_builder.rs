use std::collections::BTreeMap;

use marrow::{
    array::{Array, MapArray},
    datatypes::MapMeta,
};
use serde::Serialize;

use crate::internal::{
    error::{set_default, try_, Context, ContextSupport, Result},
    utils::{
        array_ext::{ArrayExt, OffsetsArray, SeqArrayExt},
        Mut,
    },
};

use super::{array_builder::ArrayBuilder, simple_serializer::SimpleSerializer};

#[derive(Debug, Clone)]
pub struct MapBuilder {
    pub path: String,
    pub meta: MapMeta,
    pub keys: Box<ArrayBuilder>,
    pub values: Box<ArrayBuilder>,
    pub offsets: OffsetsArray<i32>,
}

impl MapBuilder {
    pub fn new(
        path: String,
        meta: MapMeta,
        keys: ArrayBuilder,
        values: ArrayBuilder,
        is_nullable: bool,
    ) -> Result<Self> {
        Ok(Self {
            path,
            meta,
            offsets: OffsetsArray::new(is_nullable),
            keys: Box::new(keys),
            values: Box::new(values),
        })
    }

    pub fn take(&mut self) -> ArrayBuilder {
        ArrayBuilder::Map(Self {
            path: self.path.clone(),
            meta: self.meta.clone(),
            offsets: self.offsets.take(),
            keys: Box::new(self.keys.take()),
            values: Box::new(self.values.take()),
        })
    }

    pub fn is_nullable(&self) -> bool {
        self.offsets.validity.is_some()
    }

    pub fn into_array(self) -> Result<Array> {
        Ok(Array::Map(MapArray {
            meta: self.meta,
            keys: Box::new((*self.keys).into_array()?),
            values: Box::new((*self.values).into_array()?),
            validity: self.offsets.validity,
            offsets: self.offsets.offsets,
        }))
    }

    pub fn reserve(&mut self, additional: usize) {
        self.offsets.reserve(additional);
        // NOTE: do no reserve keys + values as number of elements is unclear
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
        try_(|| self.offsets.push_seq_default()).ctx(self)
    }

    fn serialize_none(&mut self) -> Result<()> {
        try_(|| self.offsets.push_seq_none()).ctx(self)
    }

    fn serialize_map_start(&mut self, _: Option<usize>) -> Result<()> {
        try_(|| self.offsets.start_seq()).ctx(self)
    }

    fn serialize_map_key<V: Serialize + ?Sized>(&mut self, key: &V) -> Result<()> {
        try_(|| {
            self.offsets.push_seq_elements(1)?;
            key.serialize(Mut(self.keys.as_mut()))
        })
        .ctx(self)
    }

    fn serialize_map_value<V: Serialize + ?Sized>(&mut self, value: &V) -> Result<()> {
        try_(|| value.serialize(Mut(self.values.as_mut()))).ctx(self)
    }

    fn serialize_map_end(&mut self) -> Result<()> {
        try_(|| self.offsets.end_seq()).ctx(self)
    }
}
