use crate::internal::{
    error::Result,
    utils::decimal::{self, DecimalParser},
};

use super::utils::{push_validity, push_validity_default, MutableBitBuffer, SimpleSerializer};

#[derive(Debug, Clone)]
pub struct DecimalBuilder {
    pub precision: u8,
    pub scale: i8,
    pub validity: Option<MutableBitBuffer>,
    pub buffer: Vec<i128>,
    pub f32_factor: f32,
    pub f64_factor: f64,
    pub parser: DecimalParser,
}

impl DecimalBuilder {
    pub fn new(precision: u8, scale: i8, nullable: bool) -> Self {
        Self {
            precision,
            scale,
            validity: nullable.then(MutableBitBuffer::default),
            buffer: Vec::new(),
            f32_factor: (10.0_f32).powi(scale as i32),
            f64_factor: (10.0_f64).powi(scale as i32),
            parser: DecimalParser::new(precision, scale, true),
        }
    }

    pub fn take(&mut self) -> Self {
        Self {
            precision: self.precision,
            scale: self.scale,
            validity: self.validity.as_mut().map(std::mem::take),
            buffer: std::mem::take(&mut self.buffer),
            f32_factor: self.f32_factor,
            f64_factor: self.f64_factor,
            parser: self.parser,
        }
    }

    pub fn is_nullable(&self) -> bool {
        self.validity.is_some()
    }
}

impl SimpleSerializer for DecimalBuilder {
    fn name(&self) -> &str {
        "DecimalBuilder"
    }

    fn serialize_default(&mut self) -> Result<()> {
        push_validity_default(&mut self.validity);
        self.buffer.push(0);
        Ok(())
    }

    fn serialize_none(&mut self) -> Result<()> {
        push_validity(&mut self.validity, false)?;
        self.buffer.push(0);
        Ok(())
    }

    fn serialize_f32(&mut self, v: f32) -> Result<()> {
        push_validity(&mut self.validity, true)?;
        self.buffer.push((v * self.f32_factor) as i128);
        Ok(())
    }

    fn serialize_f64(&mut self, v: f64) -> Result<()> {
        push_validity(&mut self.validity, true)?;
        self.buffer.push((v * self.f64_factor) as i128);
        Ok(())
    }

    fn serialize_str(&mut self, v: &str) -> Result<()> {
        let mut parse_buffer = [0; decimal::BUFFER_SIZE_I128];
        let val = self
            .parser
            .parse_decimal128(&mut parse_buffer, v.as_bytes())?;

        push_validity(&mut self.validity, true)?;
        self.buffer.push(val);
        Ok(())
    }
}

/*

    fn accept_f32(
        &self,
        _: &Structure,
        context: &mut SerializationContext,
        val: f32,
    ) -> Result<usize> {

    }

    fn accept_f64(
        &self,
        _: &Structure,
        context: &mut SerializationContext,
        val: f64,
    ) -> Result<usize> {
        let val = (val * self.f64_factor) as i128;
        context.buffers.u128[self.idx].push(ToBytes::to_bytes(val));
        Ok(self.next)
    }

    fn accept_str(
        &self,
        _: &Structure,
        context: &mut SerializationContext,
        val: &str,
    ) -> Result<usize> {
        let mut buffer = [0; decimal::BUFFER_SIZE_I128];
        context.buffers.u128[self.idx].push(ToBytes::to_bytes(val));
        Ok(self.next)
    }
*/
