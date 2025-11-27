use std::collections::{BTreeMap, HashMap};

use marrow::{
    array::{Array, DictionaryArray},
    datatypes::FieldMeta,
};
use serde::{Serialize, Serializer};

use crate::internal::{
    error::{fail, set_default, try_, Context, ContextSupport, Result},
    serialization::utils::impl_serializer,
    utils::array_view_ext::ViewExt,
};

use super::array_builder::ArrayBuilder;

#[derive(Debug, Clone)]
pub struct DictionaryUtf8Builder {
    pub name: String,
    pub indices: Box<ArrayBuilder>,
    pub values: Box<ArrayBuilder>,
    pub index: HashMap<String, usize>,
    metadata: HashMap<String, String>,
}

impl DictionaryUtf8Builder {
    pub fn new(
        name: String,
        indices: ArrayBuilder,
        values: ArrayBuilder,
        metadata: HashMap<String, String>,
    ) -> Self {
        Self {
            name,
            indices: Box::new(indices),
            values: Box::new(values),
            index: HashMap::new(),
            metadata,
        }
    }

    pub fn take(&mut self) -> ArrayBuilder {
        ArrayBuilder::DictionaryUtf8(Self {
            name: self.name.clone(),
            metadata: self.metadata.clone(),
            indices: Box::new(self.indices.take()),
            values: Box::new(self.values.take()),
            index: std::mem::take(&mut self.index),
        })
    }

    pub fn is_nullable(&self) -> bool {
        self.indices.is_nullable()
    }

    pub fn into_array_and_field_meta(mut self) -> Result<(Array, FieldMeta)> {
        let meta = FieldMeta {
            name: self.name,
            metadata: self.metadata,
            nullable: self.indices.is_nullable(),
        };

        let (keys, _) = (*self.indices).into_array_and_field_meta()?;
        let keys = Box::new(keys);

        let has_non_null_keys = !keys.as_view().is_nullable()? && keys.as_view().len()? != 0;
        let has_no_values = self.index.is_empty();

        if has_non_null_keys && has_no_values {
            // the non-null keys must be dummy values, map them to empty strings to ensure they can
            // be decoded
            self.values.serialize_str("")?;
        }

        let (values, _) = (*self.values).into_array_and_field_meta()?;
        let values = Box::new(values);

        let array = Array::Dictionary(DictionaryArray { keys, values });

        Ok((array, meta))
    }

    pub fn reserve(&mut self, additional: usize) {
        self.indices.reserve(additional);
    }

    pub fn serialize_default_value(&mut self) -> Result<()> {
        try_(|| self.indices.serialize_default_value()).ctx(self)
    }

    pub fn serialize_value<V: Serialize>(&mut self, value: V) -> Result<()> {
        value.serialize(&mut *self).ctx(self)
    }
}

impl Context for DictionaryUtf8Builder {
    fn annotate(&self, annotations: &mut BTreeMap<String, String>) {
        set_default(annotations, "field", &self.name);
        set_default(annotations, "data_type", "Dictionary(..)");
    }
}

impl<'a> Serializer for &'a mut DictionaryUtf8Builder {
    impl_serializer!(
        'a, DictionaryUtf8Builder;
        override serialize_none,
        override serialize_str,
        override serialize_unit_variant,
        override serialize_tuple_variant,
        override serialize_newtype_variant,
        override serialize_struct_variant,
    );

    fn serialize_none(self) -> Result<()> {
        serde::Serializer::serialize_none(self.indices.as_mut())
    }

    fn serialize_str(self, v: &str) -> Result<()> {
        let idx = match self.index.get(v) {
            Some(idx) => *idx,
            None => {
                let idx = self.index.len();
                self.values.serialize_str(v)?;
                self.index.insert(v.to_string(), idx);
                idx
            }
        };
        idx.serialize(self.indices.as_mut())
    }

    fn serialize_unit_variant(self, _: &'static str, _: u32, variant: &'static str) -> Result<()> {
        self.serialize_str(variant)
    }

    fn serialize_newtype_variant<V: serde::Serialize + ?Sized>(
        self,
        _: &'static str,
        _: u32,
        _: &'static str,
        _: &V,
    ) -> Result<()> {
        fail!("Cannot serialize enum with data as string");
    }

    fn serialize_tuple_variant(
        self,
        _: &'static str,
        _: u32,
        _: &'static str,
        _: usize,
    ) -> Result<Self::SerializeTupleVariant> {
        fail!("Cannot serialize enum with data as string");
    }

    fn serialize_struct_variant(
        self,
        _: &'static str,
        _: u32,
        _: &'static str,
        _: usize,
    ) -> Result<Self::SerializeStructVariant> {
        fail!("Cannot serialize enum with data as string");
    }
}
