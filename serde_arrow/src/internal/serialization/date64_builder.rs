use crate::internal::{
    error::{Error, Result},
    schema::{GenericDataType, GenericField, GenericTimeUnit},
};

use super::utils::{push_validity, push_validity_default, MutableBitBuffer, SimpleSerializer};

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
        let date_time = if self.utc {
            use chrono::{DateTime, Utc};
            v.parse::<DateTime<Utc>>()?
        } else {
            use chrono::NaiveDateTime;
            v.parse::<NaiveDateTime>()?.and_utc()
        };

        let timestamp = match self.field.data_type {
            GenericDataType::Timestamp(GenericTimeUnit::Nanosecond, _) => {
                date_time
                    .timestamp_nanos_opt()
                    .ok_or_else(|| Error::custom(format!("Timestamp '{v}' cannot be converted to nanoseconds. The dates that can be represented as nanoseconds are between 1677-09-21T00:12:44.0 and 2262-04-11T23:47:16.854775804.")))?
            },
            GenericDataType::Timestamp(GenericTimeUnit::Microsecond, _) => date_time.timestamp_micros(),
            GenericDataType::Timestamp(GenericTimeUnit::Millisecond, _) => date_time.timestamp_millis(),
            GenericDataType::Timestamp(GenericTimeUnit::Second, _) => date_time.timestamp(),
            _ => date_time.timestamp_millis(),
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
