use chrono::Timelike;

use crate::internal::{
    common::MutableBitBuffer,
    error::{Error, Result},
    schema::{GenericField, GenericTimeUnit},
};

use super::utils::{push_validity, push_validity_default, SimpleSerializer};

#[derive(Debug, Clone)]
pub struct TimeBuilder<I> {
    pub field: GenericField,
    pub validity: Option<MutableBitBuffer>,
    pub buffer: Vec<I>,
    pub seconds_factor: i64,
    pub nanoseconds_factor: i64,
}

impl<I> TimeBuilder<I> {
    pub fn new(field: GenericField, nullable: bool, unit: GenericTimeUnit) -> Self {
        let (seconds_factor, nanoseconds_factor) = unit.get_factors();

        Self {
            field,
            validity: nullable.then(MutableBitBuffer::default),
            buffer: Vec::new(),
            seconds_factor,
            nanoseconds_factor,
        }
    }

    pub fn take(&mut self) -> Self {
        Self {
            field: self.field.clone(),
            validity: self.validity.as_mut().map(std::mem::take),
            buffer: std::mem::take(&mut self.buffer),
            seconds_factor: self.seconds_factor,
            nanoseconds_factor: self.nanoseconds_factor,
        }
    }

    pub fn is_nullable(&self) -> bool {
        self.validity.is_some()
    }
}

impl<I> SimpleSerializer for TimeBuilder<I>
where
    I: TryFrom<i64> + TryFrom<i32> + Default,
    Error: From<<I as TryFrom<i32>>::Error>,
    Error: From<<I as TryFrom<i64>>::Error>,
{
    fn name(&self) -> &str {
        "Time64Builder"
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

    fn serialize_str(&mut self, v: &str) -> Result<()> {
        use chrono::naive::NaiveTime;
        let time = v.parse::<NaiveTime>()?;
        let timestamp = time.num_seconds_from_midnight() as i64 * self.seconds_factor
            + time.nanosecond() as i64 / self.nanoseconds_factor;

        push_validity(&mut self.validity, true)?;
        self.buffer.push(timestamp.try_into()?);
        Ok(())
    }

    fn serialize_i32(&mut self, v: i32) -> Result<()> {
        push_validity(&mut self.validity, true)?;
        self.buffer.push(v.try_into()?);
        Ok(())
    }

    fn serialize_i64(&mut self, v: i64) -> Result<()> {
        push_validity(&mut self.validity, true)?;
        self.buffer.push(v.try_into()?);
        Ok(())
    }
}
