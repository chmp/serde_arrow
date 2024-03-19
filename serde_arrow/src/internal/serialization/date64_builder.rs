use crate::{
    internal::{common::MutableBitBuffer, schema::GenericField},
    Result,
};

use super::utils::{push_validity, push_validity_default, SimpleSerializer};

#[derive(Debug, Clone)]
pub struct Date64Builder {
    pub field: GenericField,
    pub utc: bool,
    pub validity: Option<MutableBitBuffer>,
    pub buffer: Vec<i64>,
}

impl Date64Builder {
    pub fn new(field: GenericField, utc: bool, nullable: bool) -> Self {
        Self {
            field,
            utc,
            validity: nullable.then(MutableBitBuffer::default),
            buffer: Vec::new(),
        }
    }

    pub fn take(&mut self) -> Self {
        Self {
            field: self.field.clone(),
            utc: self.utc,
            validity: self.validity.as_mut().map(std::mem::take),
            buffer: std::mem::take(&mut self.buffer),
        }
    }

    pub fn is_nullable(&self) -> bool {
        self.validity.is_some()
    }
}

impl SimpleSerializer for Date64Builder {
    fn name(&self) -> &str {
        "Date64Builder"
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
        let timestamp = if self.utc {
            use chrono::{DateTime, Utc};
            v.parse::<DateTime<Utc>>()?.timestamp_millis()
        } else {
            use chrono::NaiveDateTime;
            v.parse::<NaiveDateTime>()?.timestamp_millis()
        };
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
