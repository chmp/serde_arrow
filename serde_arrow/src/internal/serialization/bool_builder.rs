use std::collections::BTreeMap;

use marrow::array::{Array, BooleanArray};

use crate::internal::{
    error::{set_default, try_, Context, ContextSupport, Result},
    utils::array_ext::{set_bit_buffer, set_validity, set_validity_default},
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
    fn annotate(&self, annotations: &mut BTreeMap<String, String>) {
        set_default(annotations, "field", &self.path);
        set_default(annotations, "data_type", "Boolean");
    }
}

impl SimpleSerializer for BoolBuilder {
    fn serialize_default(&mut self) -> Result<()> {
        try_(|| {
            set_validity_default(self.array.validity.as_mut(), self.array.len);
            set_bit_buffer(&mut self.array.values, self.array.len, false);
            self.array.len += 1;
            Ok(())
        })
        .ctx(self)
    }

    fn serialize_none(&mut self) -> Result<()> {
        try_(|| {
            set_validity(self.array.validity.as_mut(), self.array.len, false)?;
            set_bit_buffer(&mut self.array.values, self.array.len, false);
            self.array.len += 1;
            Ok(())
        })
        .ctx(self)
    }

    fn serialize_bool(&mut self, v: bool) -> Result<()> {
        try_(|| {
            set_validity(self.array.validity.as_mut(), self.array.len, true)?;
            set_bit_buffer(&mut self.array.values, self.array.len, v);
            self.array.len += 1;
            Ok(())
        })
        .ctx(self)
    }
}
