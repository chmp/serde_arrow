use crate::{
    internal::{common::MutableBitBuffer, schema::GenericField},
    Result,
};
use chrono::Timelike;

use super::utils::{push_validity, push_validity_default, SimpleSerializer};

#[derive(Debug, Clone)]
pub struct Time64Builder {
    pub field: GenericField,
    pub validity: Option<MutableBitBuffer>,
    pub buffer: Vec<u64>,
}

impl Time64Builder {
    pub fn new(field: GenericField, nullable: bool) -> Self {
        Self {
            field,
            validity: nullable.then(MutableBitBuffer::default),
            buffer: Vec::new(),
        }
    }

    pub fn take(&mut self) -> Self {
        Self {
            field: self.field.clone(),
            validity: self.validity.as_mut().map(std::mem::take),
            buffer: std::mem::take(&mut self.buffer),
        }
    }

    pub fn is_nullable(&self) -> bool {
        self.validity.is_some()
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
        let timestamp = {
            use chrono::naive::NaiveTime;
            let time = v.parse::<NaiveTime>()?;
            time.num_seconds_from_midnight() as u64 * 1_000_000_000u64 + time.nanosecond() as u64
        };
        push_validity(&mut self.validity, true)?;
        self.buffer.push(timestamp);
        Ok(())
    }

    fn serialize_u64(&mut self, v: u64) -> Result<()> {
        push_validity(&mut self.validity, true)?;
        self.buffer.push(v);
        Ok(())
    }
}
