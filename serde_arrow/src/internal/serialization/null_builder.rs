use std::collections::BTreeMap;

use crate::internal::{
    arrow::{Array, NullArray},
    error::{Context, Result},
    utils::btree_map,
};

use super::{array_builder::ArrayBuilder, simple_serializer::SimpleSerializer};

#[derive(Debug, Clone)]
pub struct NullBuilder {
    pub path: String,
    pub count: usize,
}

impl NullBuilder {
    pub fn new(path: String) -> Self {
        Self { path, count: 0 }
    }

    pub fn take(&mut self) -> ArrayBuilder {
        ArrayBuilder::Null(Self {
            path: self.path.clone(),
            count: std::mem::take(&mut self.count),
        })
    }

    pub fn is_nullable(&self) -> bool {
        true
    }

    pub fn into_array(self) -> Result<Array> {
        Ok(Array::Null(NullArray { len: self.count }))
    }
}

impl Context for NullBuilder {
    fn annotations(&self) -> BTreeMap<String, String> {
        btree_map!("field" => self.path.clone(), "data_type" => "Null")
    }
}

impl SimpleSerializer for NullBuilder {
    fn serialize_default(&mut self) -> Result<()> {
        self.count += 1;
        Ok(())
    }

    fn serialize_none(&mut self) -> Result<()> {
        self.count += 1;
        Ok(())
    }

    fn serialize_unit_struct(&mut self, _: &'static str) -> Result<()> {
        self.count += 1;
        Ok(())
    }
}
