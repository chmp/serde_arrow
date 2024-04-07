use chrono::Timelike;

use crate::{
    internal::{
        common::MutableBitBuffer,
        schema::{GenericField, GenericTimeUnit},
    },
    Result,
};

use super::utils::{push_validity, push_validity_default, SimpleSerializer};

#[derive(Debug, Clone)]
pub struct Time64Builder {
    pub field: GenericField,
    pub validity: Option<MutableBitBuffer>,
    pub buffer: Vec<i64>,
    pub seconds_factor: i64,
    pub nanoseconds_factor: i64,
}

impl Time64Builder {
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

    pub fn reserve(&mut self, num_elements: usize) -> Result<()> {
        if let Some(validity) = self.validity.as_mut() {
            validity.reserve(num_elements);
        }
        self.buffer.reserve(num_elements);
        Ok(())
    }
}

impl SimpleSerializer for Time64Builder {
    fn name(&self) -> &str {
        "Time64Builder"
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

    fn serialize_str(&mut self, v: &str) -> Result<()> {
        use chrono::naive::NaiveTime;
        let time = v.parse::<NaiveTime>()?;
        let timestamp = time.num_seconds_from_midnight() as i64 * self.seconds_factor
            + time.nanosecond() as i64 / self.nanoseconds_factor;

        push_validity(&mut self.validity, true)?;
        self.buffer.push(timestamp);
        Ok(())
    }

    fn serialize_i64(&mut self, v: i64) -> Result<()> {
        push_validity(&mut self.validity, true)?;
        self.buffer.push(v);
        Ok(())
    }
}
