use std::collections::BTreeMap;

use marrow::{
    array::{Array, MapArray},
    datatypes::MapMeta,
};
use serde::Serialize;

use crate::internal::{
    error::{set_default, try_, Context, ContextSupport, Error, Result},
    serialization::utils::impl_serializer,
    utils::array_ext::{ArrayExt, OffsetsArray, SeqArrayExt},
};

use super::array_builder::ArrayBuilder;

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
    }

    pub fn serialize_default_value(&mut self) -> Result<()> {
        try_(|| self.offsets.push_seq_default()).ctx(self)
    }
}

impl Context for MapBuilder {
    fn annotate(&self, annotations: &mut BTreeMap<String, String>) {
        set_default(annotations, "field", &self.path);
        set_default(annotations, "data_type", "Map(..)");
    }
}

impl<'a> serde::Serializer for &'a mut MapBuilder {
    impl_serializer!(
        'a, MapBuilder;
        override serialize_none,
        override serialize_map,
    );

    fn serialize_none(self) -> Result<()> {
        self.offsets.push_seq_none().ctx(self)
    }

    fn serialize_map(self, len: Option<usize>) -> Result<Self::SerializeMap> {
        if let Some(len) = len {
            self.keys.reserve(len);
            self.values.reserve(len);
        }
        self.offsets.start_seq().ctx(self)?;
        Ok(Self::SerializeMap::Map(self))
    }
}

impl serde::ser::SerializeMap for &mut MapBuilder {
    type Ok = ();
    type Error = Error;

    fn serialize_key<T: ?Sized + Serialize>(&mut self, key: &T) -> Result<()> {
        try_(|| {
            self.offsets.push_seq_elements(1)?;
            key.serialize(self.keys.as_mut())
        })
        .ctx(*self)
    }

    fn serialize_value<T: ?Sized + Serialize>(&mut self, value: &T) -> Result<()> {
        value.serialize(self.values.as_mut()).ctx(*self)
    }

    fn end(self) -> Result<()> {
        self.offsets.end_seq().ctx(self)
    }
}
