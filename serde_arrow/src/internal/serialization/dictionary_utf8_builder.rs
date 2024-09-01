use std::collections::{BTreeMap, HashMap};

use serde::Serialize;

use crate::internal::{
    arrow::{Array, DictionaryArray},
    error::{fail, Context, Error, Result},
    utils::{btree_map, Mut},
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

    pub fn take(&mut self) -> Self {
        Self {
            path: self.path.clone(),
            indices: Box::new(self.indices.take()),
            values: Box::new(self.values.take()),
            index: std::mem::take(&mut self.index),
        }
    }

    pub fn is_nullable(&self) -> bool {
        self.indices.is_nullable()
    }

    pub fn into_array(self) -> Result<Array> {
        Ok(Array::Dictionary(DictionaryArray {
            indices: Box::new((*self.indices).into_array()?),
            values: Box::new((*self.values).into_array()?),
        }))
    }
}

impl Context for DictionaryUtf8Builder {
    fn annotations(&self) -> BTreeMap<String, String> {
        btree_map!("field" => self.path.clone())
    }
}

impl SimpleSerializer for DictionaryUtf8Builder {
    fn name(&self) -> &str {
        "DictionaryUtf8"
    }

    fn annotate_error(&self, err: Error) -> Error {
        err.annotate_unannotated(|annotations| {
            annotations.insert(String::from("field"), self.path.clone());
        })
    }

    fn serialize_default(&mut self) -> Result<()> {
        self.indices.serialize_none()
    }

    fn serialize_none(&mut self) -> Result<()> {
        self.indices.serialize_none()
    }

    fn serialize_str(&mut self, v: &str) -> Result<()> {
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
    }

    fn serialize_unit_variant(
        &mut self,
        _: &'static str,
        _: u32,
        variant: &'static str,
    ) -> Result<()> {
        self.serialize_str(variant)
    }

    fn serialize_tuple_variant_start<'this>(
        &'this mut self,
        _: &'static str,
        _: u32,
        _: &'static str,
        _: usize,
    ) -> Result<&'this mut super::ArrayBuilder> {
        fail!("Cannot serialize enum with data as string");
    }

    fn serialize_struct_variant_start<'this>(
        &'this mut self,
        _: &'static str,
        _: u32,
        _: &'static str,
        _: usize,
    ) -> Result<&'this mut super::ArrayBuilder> {
        fail!("Cannot serialize enum with data as string");
    }

    fn serialize_newtype_variant<V: serde::Serialize + ?Sized>(
        &mut self,
        _: &'static str,
        _: u32,
        _: &'static str,
        _: &V,
    ) -> Result<()> {
        fail!("Cannot serialize enum with data as string");
    }
}
