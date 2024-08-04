use crate::internal::{
    arrow::{Array, PrimitiveArray, TimeArray, TimeUnit},
    error::Result,
};

use super::{
    array_ext::{new_primitive_array, ArrayExt, ScalarArrayExt},
    simple_serializer::SimpleSerializer,
};

#[derive(Debug, Clone)]
pub struct DurationBuilder {
    pub unit: TimeUnit,
    pub array: PrimitiveArray<i64>,
}

impl DurationBuilder {
    pub fn new(unit: TimeUnit, is_nullable: bool) -> Self {
        Self {
            unit,
            array: new_primitive_array(is_nullable),
        }
    }

    pub fn take(&mut self) -> Self {
        Self {
            unit: self.unit,
            array: self.array.take(),
        }
    }

    pub fn is_nullable(&self) -> bool {
        self.array.validity.is_some()
    }

    pub fn into_array(self) -> Result<Array> {
        Ok(Array::Duration(TimeArray {
            unit: self.unit,
            validity: self.array.validity,
            values: self.array.values,
        }))
    }
}

impl SimpleSerializer for DurationBuilder {
    fn name(&self) -> &str {
        "DurationBuilder"
    }

    fn serialize_default(&mut self) -> Result<()> {
        self.array.push_scalar_default()
    }

    fn serialize_none(&mut self) -> Result<()> {
        self.array.push_scalar_none()
    }

    fn serialize_i8(&mut self, v: i8) -> Result<()> {
        self.array.push_scalar_value(i64::from(v))
    }

    fn serialize_i16(&mut self, v: i16) -> Result<()> {
        self.array.push_scalar_value(i64::from(v))
    }

    fn serialize_i32(&mut self, v: i32) -> Result<()> {
        self.array.push_scalar_value(i64::from(v))
    }

    fn serialize_i64(&mut self, v: i64) -> Result<()> {
        self.array.push_scalar_value(v)
    }

    fn serialize_u8(&mut self, v: u8) -> Result<()> {
        self.array.push_scalar_value(i64::from(v))
    }

    fn serialize_u16(&mut self, v: u16) -> Result<()> {
        self.array.push_scalar_value(i64::from(v))
    }

    fn serialize_u32(&mut self, v: u32) -> Result<()> {
        self.array.push_scalar_value(i64::from(v))
    }

    fn serialize_u64(&mut self, v: u64) -> Result<()> {
        self.array.push_scalar_value(i64::try_from(v)?)
    }
}
