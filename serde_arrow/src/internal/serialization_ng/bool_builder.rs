use crate::{internal::common::MutableBitBuffer, Result};

use super::utils::{push_validity, push_validity_default, Mut, SimpleSerializer};

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

    fn serialize_some<V: serde::Serialize + ?Sized>(&mut self, value: &V) -> Result<()> {
        value.serialize(Mut(self))
    }

    fn serialize_bool(&mut self, v: bool) -> Result<()> {
        push_validity(&mut self.validity, true)?;
        self.buffer.push(v);
        Ok(())
    }
}
