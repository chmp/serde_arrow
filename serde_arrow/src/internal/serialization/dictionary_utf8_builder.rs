use std::collections::HashMap;

use serde::Serialize;

use crate::internal::{common::Mut, error::Result, schema::GenericField};

use super::{array_builder::ArrayBuilder, utils::SimpleSerializer};

#[derive(Debug, Clone)]
pub struct DictionaryUtf8Builder {
    pub field: GenericField,
    pub indices: Box<ArrayBuilder>,
    pub values: Box<ArrayBuilder>,
    pub index: HashMap<String, usize>,
}

impl DictionaryUtf8Builder {
    pub fn new(field: GenericField, indices: ArrayBuilder, values: ArrayBuilder) -> Self {
        Self {
            field,
            indices: Box::new(indices),
            values: Box::new(values),
            index: HashMap::new(),
        }
    }

    pub fn take(&mut self) -> Self {
        Self {
            field: self.field.clone(),
            indices: Box::new(self.indices.take()),
            values: Box::new(self.values.take()),
            index: std::mem::take(&mut self.index),
        }
    }

    pub fn is_nullable(&self) -> bool {
        self.indices.is_nullable()
    }

    pub fn reserve(&mut self, num_elements: usize) -> Result<()> {
        self.indices.reserve(num_elements)
    }
}

impl SimpleSerializer for DictionaryUtf8Builder {
    fn name(&self) -> &str {
        "DictionaryUtf8"
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
}
