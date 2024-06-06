use chrono::{NaiveDate, NaiveDateTime};

use crate::internal::{error::Result, schema::GenericField};

use super::utils::{push_validity, push_validity_default, MutableBitBuffer, SimpleSerializer};

#[derive(Debug, Clone)]
pub struct Date32Builder {
    pub field: GenericField,
    pub validity: Option<MutableBitBuffer>,
    pub buffer: Vec<i32>,
}

impl Date32Builder {
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

impl SimpleSerializer for Date32Builder {
    fn name(&self) -> &str {
        "Date32Builder"
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
        const UNIX_EPOCH: NaiveDate = NaiveDateTime::UNIX_EPOCH.date();

        let date = v.parse::<NaiveDate>()?;
        let duration_since_epoch = date.signed_duration_since(UNIX_EPOCH);
        let days_since_epoch = duration_since_epoch.num_days().try_into()?;

        push_validity(&mut self.validity, true)?;
        self.buffer.push(days_since_epoch);
        Ok(())
    }

    fn serialize_i32(&mut self, v: i32) -> Result<()> {
        push_validity(&mut self.validity, true)?;
        self.buffer.push(v);
        Ok(())
    }
}
