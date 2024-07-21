use half::f16;

use crate::internal::{
    arrow::{Array, PrimitiveArray},
    error::Result,
    utils::Mut,
};

use super::utils::{push_validity, push_validity_default, MutableBitBuffer, SimpleSerializer};

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

    pub fn is_nullable(&self) -> bool {
        self.validity.is_some()
    }

    fn serialize_value(&mut self, value: I) -> Result<()> {
        push_validity(&mut self.validity, true)?;
        self.buffer.push(value);
        Ok(())
    }
}

macro_rules! impl_into_array {
    ($ty:ty, $var:ident) => {
        impl FloatBuilder<$ty> {
            pub fn into_array(self) -> Result<Array> {
                Ok(Array::$var(PrimitiveArray {
                    validity: self.validity.map(|b| b.buffer),
                    values: self.buffer,
                }))
            }
        }
    };
}

impl_into_array!(f16, Float16);
impl_into_array!(f32, Float32);
impl_into_array!(f64, Float64);

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

    fn serialize_i8(&mut self, v: i8) -> Result<()> {
        self.serialize_value(v as f32)
    }

    fn serialize_i16(&mut self, v: i16) -> Result<()> {
        self.serialize_value(v as f32)
    }

    fn serialize_i32(&mut self, v: i32) -> Result<()> {
        self.serialize_value(v as f32)
    }

    fn serialize_i64(&mut self, v: i64) -> Result<()> {
        self.serialize_value(v as f32)
    }

    fn serialize_u8(&mut self, v: u8) -> Result<()> {
        self.serialize_value(v as f32)
    }

    fn serialize_u16(&mut self, v: u16) -> Result<()> {
        self.serialize_value(v as f32)
    }

    fn serialize_u32(&mut self, v: u32) -> Result<()> {
        self.serialize_value(v as f32)
    }

    fn serialize_u64(&mut self, v: u64) -> Result<()> {
        self.serialize_value(v as f32)
    }

    fn serialize_f32(&mut self, v: f32) -> Result<()> {
        self.serialize_value(v)
    }

    fn serialize_f64(&mut self, v: f64) -> Result<()> {
        self.serialize_value(v as f32)
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

    fn serialize_i8(&mut self, v: i8) -> Result<()> {
        self.serialize_value(v as f64)
    }

    fn serialize_i16(&mut self, v: i16) -> Result<()> {
        self.serialize_value(v as f64)
    }

    fn serialize_i32(&mut self, v: i32) -> Result<()> {
        self.serialize_value(v as f64)
    }

    fn serialize_i64(&mut self, v: i64) -> Result<()> {
        self.serialize_value(v as f64)
    }

    fn serialize_u8(&mut self, v: u8) -> Result<()> {
        self.serialize_value(v as f64)
    }

    fn serialize_u16(&mut self, v: u16) -> Result<()> {
        self.serialize_value(v as f64)
    }

    fn serialize_u32(&mut self, v: u32) -> Result<()> {
        self.serialize_value(v as f64)
    }

    fn serialize_u64(&mut self, v: u64) -> Result<()> {
        self.serialize_value(v as f64)
    }

    fn serialize_f32(&mut self, v: f32) -> Result<()> {
        self.serialize_value(v as f64)
    }

    fn serialize_f64(&mut self, v: f64) -> Result<()> {
        self.serialize_value(v)
    }
}

impl SimpleSerializer for FloatBuilder<f16> {
    fn name(&self) -> &str {
        "FloatBuilder<f16>"
    }

    fn serialize_default(&mut self) -> Result<()> {
        push_validity_default(&mut self.validity);
        self.buffer.push(f16::ZERO);
        Ok(())
    }

    fn serialize_none(&mut self) -> Result<()> {
        push_validity(&mut self.validity, false)?;
        self.buffer.push(f16::ZERO);
        Ok(())
    }

    fn serialize_f32(&mut self, v: f32) -> Result<()> {
        self.serialize_value(f16::from_f32(v))
    }

    fn serialize_f64(&mut self, v: f64) -> Result<()> {
        self.serialize_value(f16::from_f64(v))
    }
}
