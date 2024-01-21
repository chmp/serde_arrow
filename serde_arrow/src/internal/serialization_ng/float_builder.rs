use crate::{internal::common::MutableBitBuffer, Result};

use super::utils::{push_validity, push_validity_default, Mut, SimpleSerializer};

#[derive(Debug, Clone, Default)]
pub struct FloatBuilder<I> {
    pub validity: Option<MutableBitBuffer>,
    pub buffer: Vec<I>,
}

impl<I> FloatBuilder<I> {
    pub fn new(is_nullable: bool) -> Self {
        Self {
            validity: is_nullable.then(MutableBitBuffer::default),
            buffer: Default::default(),
        }
    }

    pub fn take(&mut self) -> Self {
        Self {
            validity: self.validity.as_mut().map(std::mem::take),
            buffer: std::mem::take(&mut self.buffer),
        }
    }
}

impl SimpleSerializer for FloatBuilder<f32> {
    fn name(&self) -> &str {
        "FloatBuilder<f32>"
    }

    fn serialize_default(&mut self) -> Result<()> {
        self.buffer.push(0.0);
        Ok(())
    }

    fn serialize_none(&mut self) -> Result<()> {
        push_validity(&mut self.validity, false)?;
        self.buffer.push(0.0);
        Ok(())
    }

    fn serialize_some<V: serde::Serialize + ?Sized>(&mut self, value: &V) -> Result<()> {
        value.serialize(Mut(self))
    }

    fn serialize_f32(&mut self, v: f32) -> Result<()> {
        push_validity(&mut self.validity, true)?;
        self.buffer.push(v);
        Ok(())
    }
}

impl SimpleSerializer for FloatBuilder<f64> {
    fn name(&self) -> &str {
        "FloatBuilder<64>"
    }

    fn serialize_default(&mut self) -> Result<()> {
        push_validity_default(&mut self.validity);
        self.buffer.push(0.0);
        Ok(())
    }

    fn serialize_none(&mut self) -> Result<()> {
        push_validity(&mut self.validity, false)?;
        self.buffer.push(0.0);
        Ok(())
    }

    fn serialize_some<V: serde::Serialize + ?Sized>(&mut self, value: &V) -> Result<()> {
        value.serialize(Mut(self))
    }

    fn serialize_f32(&mut self, v: f32) -> Result<()> {
        push_validity(&mut self.validity, true)?;
        self.buffer.push(v as f64);
        Ok(())
    }

    fn serialize_f64(&mut self, v: f64) -> Result<()> {
        push_validity(&mut self.validity, true)?;
        self.buffer.push(v);
        Ok(())
    }
}
