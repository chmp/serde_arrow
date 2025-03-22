use std::collections::{BTreeMap, HashMap};

use marrow::array::{Array, DictionaryArray};
use serde::Serialize;

use crate::internal::{
    error::{fail, set_default, try_, Context, ContextSupport, Result},
    utils::{Mut, array_view_ext::ViewExt},
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
}

impl Context for DictionaryUtf8Builder {
    fn annotate(&self, annotations: &mut BTreeMap<String, String>) {
        set_default(annotations, "field", &self.path);
        set_default(annotations, "data_type", "Dictionary(..)");
    }
}

impl SimpleSerializer for DictionaryUtf8Builder {
    fn serialize_default(&mut self) -> Result<()> {
        try_(|| self.indices.serialize_default()).ctx(self)
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
