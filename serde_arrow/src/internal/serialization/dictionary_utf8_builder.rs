use std::collections::{BTreeMap, HashMap};

use marrow::array::{Array, DictionaryArray};
use serde::Serialize;

use crate::internal::{
    error::{fail, set_default, try_, Context, ContextSupport, Result},
    serialization::utils::impl_serializer,
    utils::{array_view_ext::ViewExt, Mut},
};

use super::{array_builder::ArrayBuilder, simple_serializer::SimpleSerializer};

#[derive(Debug, Clone)]
pub struct DictionaryUtf8Builder {
    path: String,
    pub indices: Box<ArrayBuilder>,
    pub values: Box<ArrayBuilder>,
    pub index: HashMap<String, usize>,
}

impl DictionaryUtf8Builder {
    pub fn new(path: String, indices: ArrayBuilder, values: ArrayBuilder) -> Self {
        Self {
            path,
            indices: Box::new(indices),
            values: Box::new(values),
            index: HashMap::new(),
        }
    }

    pub fn take(&mut self) -> ArrayBuilder {
        ArrayBuilder::DictionaryUtf8(Self {
            path: self.path.clone(),
            indices: Box::new(self.indices.take()),
            values: Box::new(self.values.take()),
            index: std::mem::take(&mut self.index),
        })
    }

    pub fn is_nullable(&self) -> bool {
        self.indices.is_nullable()
    }

    pub fn into_array(mut self) -> Result<Array> {
        let keys = Box::new((*self.indices).into_array()?);

        let has_non_null_keys = !keys.as_view().is_nullable()? && keys.as_view().len()? != 0;
        let has_no_values = self.index.is_empty();

        if has_non_null_keys && has_no_values {
            // the non-null keys must be dummy values, map them to empty strings to ensure they can
            // be decoded
            self.values.serialize_str("")?;
        }

        Ok(Array::Dictionary(DictionaryArray {
            keys,
            values: Box::new((*self.values).into_array()?),
        }))
    }

    pub fn reserve(&mut self, additional: usize) {
        self.indices.reserve(additional);
    }

    pub fn serialize_default_value(&mut self) -> Result<()> {
        try_(|| self.indices.serialize_default()).ctx(self)
    }
}

impl Context for DictionaryUtf8Builder {
    fn annotate(&self, annotations: &mut BTreeMap<String, String>) {
        set_default(annotations, "field", &self.path);
        set_default(annotations, "data_type", "Dictionary(..)");
    }
}

impl SimpleSerializer for DictionaryUtf8Builder {
    fn serialize_default(&mut self) -> Result<()> {
        self.serialize_default_value()
    }

    fn serialize_none(&mut self) -> Result<()> {
        try_(|| self.indices.serialize_none().ctx(self)).ctx(self)
    }

    fn serialize_str(&mut self, v: &str) -> Result<()> {
        try_(|| {
            let idx = match self.index.get(v) {
                Some(idx) => *idx,
                None => {
                    let idx = self.index.len();
                    self.values.serialize_str(v)?;
                    self.index.insert(v.to_string(), idx);
                    idx
                }
            };
            idx.serialize(Mut(self.indices.as_mut()))
        })
        .ctx(self)
    }

    fn serialize_unit_variant(
        &mut self,
        _: &'static str,
        _: u32,
        variant: &'static str,
    ) -> Result<()> {
        try_(|| self.serialize_str(variant)).ctx(self)
    }

    fn serialize_tuple_variant_start<'this>(
        &'this mut self,
        _: &'static str,
        _: u32,
        _: &'static str,
        _: usize,
    ) -> Result<&'this mut super::ArrayBuilder> {
        fail!(in self, "Cannot serialize enum with data as string");
    }

    fn serialize_struct_variant_start<'this>(
        &'this mut self,
        _: &'static str,
        _: u32,
        _: &'static str,
        _: usize,
    ) -> Result<&'this mut super::ArrayBuilder> {
        fail!(in self, "Cannot serialize enum with data as string");
    }

    fn serialize_newtype_variant<V: serde::Serialize + ?Sized>(
        &mut self,
        _: &'static str,
        _: u32,
        _: &'static str,
        _: &V,
    ) -> Result<()> {
        fail!(in self, "Cannot serialize enum with data as string");
    }
}

impl<'a> serde::Serializer for &'a mut DictionaryUtf8Builder {
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
        try_(|| serde::Serializer::serialize_none(self.indices.as_mut()).ctx(self)).ctx(self)
    }

    fn serialize_str(self, v: &str) -> Result<()> {
        try_(|| {
            let idx = match self.index.get(v) {
                Some(idx) => *idx,
                None => {
                    let idx = self.index.len();
                    serde::Serializer::serialize_str(self.values.as_mut(), v)?;
                    self.index.insert(v.to_string(), idx);
                    idx
                }
            };
            idx.serialize(Mut(self.indices.as_mut()))
        })
        .ctx(self)
    }

    fn serialize_unit_variant(self, _: &'static str, _: u32, variant: &'static str) -> Result<()> {
        // TODO: revert back to self.serialize_str(variant)
        try_(|| serde::Serializer::serialize_str(&mut *self, variant)).ctx(self)
    }

    fn serialize_newtype_variant<V: serde::Serialize + ?Sized>(
        self,
        _: &'static str,
        _: u32,
        _: &'static str,
        _: &V,
    ) -> Result<()> {
        fail!(in self, "Cannot serialize enum with data as string");
    }

    fn serialize_tuple_variant(
        self,
        _: &'static str,
        _: u32,
        _: &'static str,
        _: usize,
    ) -> Result<Self::SerializeTupleVariant> {
        fail!(in self, "Cannot serialize enum with data as string");
    }

    fn serialize_struct_variant(
        self,
        _: &'static str,
        _: u32,
        _: &'static str,
        _: usize,
    ) -> Result<Self::SerializeStructVariant> {
        fail!(in self, "Cannot serialize enum with data as string");
    }
}
