use crate::internal::{
    arrow::{Array, TimeUnit},
    error::Result,
};

use super::utils::{push_validity, push_validity_default, MutableBitBuffer, SimpleSerializer};

#[derive(Debug, Clone)]
pub struct DurationBuilder {
    pub unit: TimeUnit,
    pub validity: Option<MutableBitBuffer>,
    pub buffer: Vec<i64>,
}

impl DurationBuilder {
    pub fn new(unit: TimeUnit, is_nullable: bool) -> Self {
        Self {
            unit,
            validity: is_nullable.then(MutableBitBuffer::default),
            buffer: Default::default(),
        }
    }

    pub fn take(&mut self) -> Self {
        Self {
            unit: self.unit,
            validity: self.validity.as_mut().map(std::mem::take),
            buffer: std::mem::take(&mut self.buffer),
        }
    }

    pub fn is_nullable(&self) -> bool {
        self.validity.is_some()
    }

    pub fn into_array(self) -> Array {
        unimplemented!()
    }
}

impl SimpleSerializer for DurationBuilder {
    fn name(&self) -> &str {
        "DurationBuilder"
    }

    fn serialize_default(&mut self) -> Result<()> {
        push_validity_default(&mut self.validity);
        self.buffer.push(i64::default());
        Ok(())
    }

    fn serialize_none(&mut self) -> Result<()> {
        push_validity(&mut self.validity, false)?;
        self.buffer.push(i64::default());
        Ok(())
    }

    fn serialize_i8(&mut self, v: i8) -> Result<()> {
        push_validity(&mut self.validity, true)?;
        self.buffer.push(i64::from(v));
        Ok(())
    }

    fn serialize_i16(&mut self, v: i16) -> Result<()> {
        push_validity(&mut self.validity, true)?;
        self.buffer.push(i64::from(v));
        Ok(())
    }

    fn serialize_i32(&mut self, v: i32) -> Result<()> {
        push_validity(&mut self.validity, true)?;
        self.buffer.push(i64::from(v));
        Ok(())
    }

    fn serialize_i64(&mut self, v: i64) -> Result<()> {
        push_validity(&mut self.validity, true)?;
        self.buffer.push(v);
        Ok(())
    }

    fn serialize_u8(&mut self, v: u8) -> Result<()> {
        push_validity(&mut self.validity, true)?;
        self.buffer.push(i64::from(v));
        Ok(())
    }

    fn serialize_u16(&mut self, v: u16) -> Result<()> {
        push_validity(&mut self.validity, true)?;
        self.buffer.push(i64::from(v));
        Ok(())
    }

    fn serialize_u32(&mut self, v: u32) -> Result<()> {
        push_validity(&mut self.validity, true)?;
        self.buffer.push(i64::from(v));
        Ok(())
    }

    fn serialize_u64(&mut self, v: u64) -> Result<()> {
        push_validity(&mut self.validity, true)?;
        self.buffer.push(i64::try_from(v)?);
        Ok(())
    }
}
