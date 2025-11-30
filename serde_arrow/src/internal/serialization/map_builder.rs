use std::collections::{BTreeMap, HashMap};

use marrow::{
    array::{Array, MapArray},
    datatypes::{FieldMeta, MapMeta},
};
use serde::{Serialize, Serializer};

use crate::internal::{
    error::{prepend, set_default, Context, ContextSupport, Error, Result},
    serialization::utils::impl_serializer,
    utils::array_ext::{ArrayExt, OffsetsArray, SeqArrayExt},
};

use super::array_builder::ArrayBuilder;

#[derive(Debug, Clone)]
pub struct MapBuilder {
    pub name: String,
    entries_name: String,
    sorted: bool,
    pub keys: Box<ArrayBuilder>,
    pub values: Box<ArrayBuilder>,
    pub offsets: OffsetsArray<i32>,
    pub metadata: HashMap<String, String>,
}

impl MapBuilder {
    pub fn new(
        name: String,
        entries_name: String,
        sorted: bool,
        keys: ArrayBuilder,
        values: ArrayBuilder,
        is_nullable: bool,
        metadata: HashMap<String, String>,
    ) -> Result<Self> {
        Ok(Self {
            name,
            entries_name,
            sorted,
            offsets: OffsetsArray::new(is_nullable),
            keys: Box::new(keys),
            values: Box::new(values),
            metadata,
        })
    }

    pub fn take(&mut self) -> ArrayBuilder {
        ArrayBuilder::Map(Self {
            name: self.name.clone(),
            entries_name: self.entries_name.clone(),
            sorted: self.sorted,
            metadata: self.metadata.clone(),
            offsets: self.offsets.take(),
            keys: Box::new(self.keys.take()),
            values: Box::new(self.values.take()),
        })
    }

    pub fn is_nullable(&self) -> bool {
        self.offsets.validity.is_some()
    }

    pub fn into_array_and_field_meta(self) -> Result<(Array, FieldMeta)> {
        let meta = FieldMeta {
            name: self.name,
            nullable: self.offsets.is_nullable(),
            metadata: self.metadata,
        };
        let (keys, keys_meta) = (*self.keys).into_array_and_field_meta()?;
        let (values, values_meta) = (*self.values).into_array_and_field_meta()?;

        let array = Array::Map(MapArray {
            meta: MapMeta {
                entries_name: self.entries_name,
                sorted: self.sorted,
                keys: keys_meta,
                values: values_meta,
            },
            keys: Box::new(keys),
            values: Box::new(values),
            validity: self.offsets.validity,
            offsets: self.offsets.offsets,
        });
        Ok((array, meta))
    }

    pub fn reserve(&mut self, additional: usize) {
        self.offsets.reserve(additional);
    }

    pub fn serialize_default_value(&mut self) -> Result<()> {
        self.offsets.push_seq_default().ctx(self)
    }

    pub fn serialize_value<V: Serialize>(&mut self, value: V) -> Result<()> {
        value.serialize(&mut *self).ctx(self)
    }
}

impl Context for MapBuilder {
    fn annotate(&self, annotations: &mut BTreeMap<String, String>) {
        prepend(annotations, "field", &self.name);
        set_default(annotations, "data_type", "Map");
    }
}

impl<'a> Serializer for &'a mut MapBuilder {
    impl_serializer!(
        'a, MapBuilder;
        override serialize_none,
        override serialize_map,
    );

    fn serialize_none(self) -> Result<()> {
        self.offsets.push_seq_none()
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
        self.offsets.push_seq_elements(1)?;
        self.keys.serialize_value(key)
    }

    fn serialize_value<T: ?Sized + Serialize>(&mut self, value: &T) -> Result<()> {
        self.values.serialize_value(value)
    }

    fn end(self) -> Result<()> {
        self.offsets.end_seq()
    }
}
