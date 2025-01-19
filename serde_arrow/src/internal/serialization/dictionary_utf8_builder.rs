use std::collections::{BTreeMap, HashMap};

use marrow::array::{Array, DictionaryArray};
use serde::Serialize;

use crate::internal::{
    error::{fail, set_default, try_, Context, ContextSupport, Result},
    utils::Mut,
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

    pub fn into_array(self) -> Result<Array> {
        Ok(Array::Dictionary(DictionaryArray {
            keys: Box::new((*self.indices).into_array()?),
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
        try_(|| self.indices.serialize_none()).ctx(self)
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
