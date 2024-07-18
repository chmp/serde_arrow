use crate::internal::{
    arrow::{Array, NullArray},
    error::Result,
};

use super::utils::SimpleSerializer;

#[derive(Debug, Clone, Default)]
pub struct NullBuilder {
    pub count: usize,
}

impl NullBuilder {
    pub fn new() -> Self {
        Self::default()
    }

    pub fn take(&mut self) -> Self {
        Self {
            count: std::mem::take(&mut self.count),
        }
    }

    pub fn is_nullable(&self) -> bool {
        true
    }

    pub fn into_array(self) -> Array {
        Array::Null(NullArray { len: self.count })
    }
}

impl SimpleSerializer for NullBuilder {
    fn name(&self) -> &str {
        "NullBuilder"
    }

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
