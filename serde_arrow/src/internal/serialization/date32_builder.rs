use chrono::{NaiveDate, NaiveDateTime};

use crate::internal::{
    arrow::{Array, PrimitiveArray},
    error::Result,
    utils::array_ext::{new_primitive_array, ArrayExt, ScalarArrayExt},
};

use super::simple_serializer::SimpleSerializer;

#[derive(Debug, Clone)]
pub struct Date32Builder(PrimitiveArray<i32>);

impl Date32Builder {
    pub fn new(is_nullable: bool) -> Self {
        Self(new_primitive_array(is_nullable))
    }

    pub fn take(&mut self) -> Self {
        Self(self.0.take())
    }

    pub fn is_nullable(&self) -> bool {
        self.0.validity.is_some()
    }

    pub fn into_array(self) -> Result<Array> {
        Ok(Array::Date32(self.0))
    }
}

impl SimpleSerializer for Date32Builder {
    fn name(&self) -> &str {
        "Date32Builder"
    }

    fn serialize_default(&mut self) -> Result<()> {
        self.0.push_scalar_default()
    }

    fn serialize_none(&mut self) -> Result<()> {
        self.0.push_scalar_none()
    }

    fn serialize_str(&mut self, v: &str) -> Result<()> {
        const UNIX_EPOCH: NaiveDate = NaiveDateTime::UNIX_EPOCH.date();

        let date = v.parse::<NaiveDate>()?;
        let duration_since_epoch = date.signed_duration_since(UNIX_EPOCH);
        let days_since_epoch = duration_since_epoch.num_days().try_into()?;

        self.0.push_scalar_value(days_since_epoch)
    }

    fn serialize_i32(&mut self, v: i32) -> Result<()> {
        self.0.push_scalar_value(v)
    }
}
