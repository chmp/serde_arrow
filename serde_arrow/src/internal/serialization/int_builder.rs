use crate::{internal::common::MutableBitBuffer, Error, Result};

use super::utils::{push_validity, push_validity_default, SimpleSerializer};

#[derive(Debug, Clone, Default)]
pub struct IntBuilder<I> {
    pub validity: Option<MutableBitBuffer>,
    pub buffer: Vec<I>,
}

impl<I> IntBuilder<I> {
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

impl<I> SimpleSerializer for IntBuilder<I>
where
    I: Default
        + TryFrom<i8>
        + TryFrom<i16>
        + TryFrom<i32>
        + TryFrom<i64>
        + TryFrom<u8>
        + TryFrom<u16>
        + TryFrom<u32>
        + TryFrom<u64>,
    Error: From<<I as TryFrom<i8>>::Error>,
    Error: From<<I as TryFrom<i16>>::Error>,
    Error: From<<I as TryFrom<i32>>::Error>,
    Error: From<<I as TryFrom<i64>>::Error>,
    Error: From<<I as TryFrom<u8>>::Error>,
    Error: From<<I as TryFrom<u16>>::Error>,
    Error: From<<I as TryFrom<u32>>::Error>,
    Error: From<<I as TryFrom<u64>>::Error>,
{
    fn name(&self) -> &str {
        "IntBuilder<()>"
    }

    fn serialize_default(&mut self) -> Result<()> {
        push_validity_default(&mut self.validity);
        self.buffer.push(I::default());
        Ok(())
    }

    fn serialize_none(&mut self) -> Result<()> {
        push_validity(&mut self.validity, false)?;
        self.buffer.push(I::default());
        Ok(())
    }

    fn serialize_i8(&mut self, v: i8) -> Result<()> {
        push_validity(&mut self.validity, true)?;
        self.buffer.push(I::try_from(v)?);
        Ok(())
    }

    fn serialize_i16(&mut self, v: i16) -> Result<()> {
        push_validity(&mut self.validity, true)?;
        self.buffer.push(I::try_from(v)?);
        Ok(())
    }

    fn serialize_i32(&mut self, v: i32) -> Result<()> {
        push_validity(&mut self.validity, true)?;
        self.buffer.push(I::try_from(v)?);
        Ok(())
    }

    fn serialize_i64(&mut self, v: i64) -> Result<()> {
        push_validity(&mut self.validity, true)?;
        self.buffer.push(I::try_from(v)?);
        Ok(())
    }

    fn serialize_u8(&mut self, v: u8) -> Result<()> {
        push_validity(&mut self.validity, true)?;
        self.buffer.push(I::try_from(v)?);
        Ok(())
    }

    fn serialize_u16(&mut self, v: u16) -> Result<()> {
        push_validity(&mut self.validity, true)?;
        self.buffer.push(I::try_from(v)?);
        Ok(())
    }

    fn serialize_u32(&mut self, v: u32) -> Result<()> {
        push_validity(&mut self.validity, true)?;
        self.buffer.push(I::try_from(v)?);
        Ok(())
    }

    fn serialize_u64(&mut self, v: u64) -> Result<()> {
        push_validity(&mut self.validity, true)?;
        self.buffer.push(I::try_from(v)?);
        Ok(())
    }

    fn serialize_char(&mut self, v: char) -> Result<()> {
        push_validity(&mut self.validity, true)?;
        self.buffer.push(I::try_from(u32::from(v))?);
        Ok(())
    }
}
