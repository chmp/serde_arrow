use crate::{internal::common::MutableBitBuffer, Result};

use super::utils::{push_validity, push_validity_default, SimpleSerializer};

#[derive(Debug, Clone)]
pub struct BoolBuilder {
    pub validity: Option<MutableBitBuffer>,
    pub buffer: MutableBitBuffer,
}

impl BoolBuilder {
    pub fn new(is_nullable: bool) -> Self {
        Self {
            validity: is_nullable.then(MutableBitBuffer::default),
            buffer: MutableBitBuffer::default(),
        }
    }

    pub fn take(&mut self) -> Self {
        Self {
            validity: self.validity.as_mut().map(std::mem::take),
            buffer: std::mem::take(&mut self.buffer),
        }
    }

    pub fn is_nullable(&self) -> bool {
        self.validity.is_some()
    }

    pub fn reserve(&mut self, num_elements: usize) -> Result<()> {
        if let Some(validity) = self.validity.as_mut() {
            validity.reserve(num_elements);
        }
        self.buffer.reserve(num_elements);
        Ok(())
    }
}

impl SimpleSerializer for BoolBuilder {
    fn name(&self) -> &str {
        "BoolBuilder"
    }

    fn serialize_default(&mut self) -> Result<()> {
        push_validity_default(&mut self.validity);
        self.buffer.push(false);
        Ok(())
    }

    fn serialize_none(&mut self) -> Result<()> {
        push_validity(&mut self.validity, false)?;
        self.buffer.push(false);
        Ok(())
    }

    fn serialize_bool(&mut self, v: bool) -> Result<()> {
        push_validity(&mut self.validity, true)?;
        self.buffer.push(v);
        Ok(())
    }
}
