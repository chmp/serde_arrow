use std::collections::BTreeMap;

use crate::internal::{
    arrow::{Array, BooleanArray},
    error::{Context, ContextSupport, Result},
    utils::{
        array_ext::{set_bit_buffer, set_validity, set_validity_default},
        btree_map,
    },
};

use super::{array_builder::ArrayBuilder, simple_serializer::SimpleSerializer};

#[derive(Debug, Clone)]
pub struct BoolBuilder {
    path: String,
    array: BooleanArray,
}

impl BoolBuilder {
    pub fn new(path: String, is_nullable: bool) -> Self {
        Self {
            path,
            array: BooleanArray {
                len: 0,
                validity: is_nullable.then(Vec::new),
                values: Vec::new(),
            },
        }
    }

    pub fn take(&mut self) -> ArrayBuilder {
        ArrayBuilder::Bool(Self {
            path: self.path.clone(),
            array: BooleanArray {
                len: std::mem::take(&mut self.array.len),
                validity: self.array.validity.as_mut().map(std::mem::take),
                values: std::mem::take(&mut self.array.values),
            },
        })
    }

    pub fn is_nullable(&self) -> bool {
        self.array.validity.is_some()
    }

    pub fn into_array(self) -> Result<Array> {
        Ok(Array::Boolean(self.array))
    }
}

impl Context for BoolBuilder {
    fn annotations(&self) -> BTreeMap<String, String> {
        btree_map!("field" => self.path.clone(), "data_type" => "Boolean")
    }
}

impl SimpleSerializer for BoolBuilder {
    fn serialize_default(&mut self) -> Result<()> {
        set_validity_default(self.array.validity.as_mut(), self.array.len);
        set_bit_buffer(&mut self.array.values, self.array.len, false);
        self.array.len += 1;
        Ok(())
    }

    fn serialize_none(&mut self) -> Result<()> {
        set_validity(self.array.validity.as_mut(), self.array.len, false).ctx(self)?;
        set_bit_buffer(&mut self.array.values, self.array.len, false);
        self.array.len += 1;
        Ok(())
    }

    fn serialize_bool(&mut self, v: bool) -> Result<()> {
        set_validity(self.array.validity.as_mut(), self.array.len, true).ctx(self)?;
        set_bit_buffer(&mut self.array.values, self.array.len, v);
        self.array.len += 1;
        Ok(())
    }
}
