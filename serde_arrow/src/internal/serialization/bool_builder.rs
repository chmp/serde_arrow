use crate::internal::{
    arrow::{Array, BooleanArray},
    error::Result,
};

use super::{
    array_ext::{set_bit_buffer, set_validity, set_validity_default},
    utils::SimpleSerializer,
};

#[derive(Debug, Clone)]
pub struct BoolBuilder(BooleanArray);

impl BoolBuilder {
    pub fn new(is_nullable: bool) -> Self {
        Self(BooleanArray {
            len: 0,
            validity: is_nullable.then(Vec::new),
            values: Vec::new(),
        })
    }

    pub fn take(&mut self) -> Self {
        Self(BooleanArray {
            len: std::mem::take(&mut self.0.len),
            validity: self.0.validity.as_mut().map(std::mem::take),
            values: std::mem::take(&mut self.0.values),
        })
    }

    pub fn is_nullable(&self) -> bool {
        self.0.validity.is_some()
    }

    pub fn into_array(self) -> Result<Array> {
        Ok(Array::Boolean(self.0))
    }
}

impl SimpleSerializer for BoolBuilder {
    fn name(&self) -> &str {
        "BoolBuilder"
    }

    fn serialize_default(&mut self) -> Result<()> {
        set_validity_default(self.0.validity.as_mut(), self.0.len);
        set_bit_buffer(&mut self.0.values, self.0.len, false);
        self.0.len += 1;
        Ok(())
    }

    fn serialize_none(&mut self) -> Result<()> {
        set_validity(self.0.validity.as_mut(), self.0.len, false)?;
        set_bit_buffer(&mut self.0.values, self.0.len, false);
        self.0.len += 1;
        Ok(())
    }

    fn serialize_bool(&mut self, v: bool) -> Result<()> {
        set_validity(self.0.validity.as_mut(), self.0.len, true)?;
        set_bit_buffer(&mut self.0.values, self.0.len, v);
        self.0.len += 1;
        Ok(())
    }
}
