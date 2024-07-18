use chrono::Timelike;

use crate::internal::{
    arrow::{Array, TimeArray, TimeUnit},
    error::{Error, Result},
    schema::GenericField,
};

use super::utils::{push_validity, push_validity_default, MutableBitBuffer, SimpleSerializer};

#[derive(Debug, Clone)]
pub struct TimeBuilder<I> {
    pub field: GenericField,
    pub validity: Option<MutableBitBuffer>,
    pub buffer: Vec<I>,
    pub unit: TimeUnit,
}

impl<I> TimeBuilder<I> {
    pub fn new(field: GenericField, nullable: bool, unit: TimeUnit) -> Self {
        Self {
            field,
            validity: nullable.then(MutableBitBuffer::default),
            buffer: Vec::new(),
            unit,
        }
    }

    pub fn take(&mut self) -> Self {
        Self {
            field: self.field.clone(),
            validity: self.validity.as_mut().map(std::mem::take),
            buffer: std::mem::take(&mut self.buffer),
            unit: self.unit,
        }
    }

    pub fn is_nullable(&self) -> bool {
        self.validity.is_some()
    }
}

impl TimeBuilder<i32> {
    pub fn into_array(self) -> Array {
        Array::Time32(TimeArray {
            unit: self.unit,
            validity: self.validity.map(|v| v.buffer),
            values: self.buffer,
        })
    }
}

impl TimeBuilder<i64> {
    pub fn into_array(self) -> Array {
        Array::Time64(TimeArray {
            unit: self.unit,
            validity: self.validity.map(|v| v.buffer),
            values: self.buffer,
        })
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
        let (seconds_factor, nanoseconds_factor) = match self.unit {
            TimeUnit::Nanosecond => (1_000_000_000, 1),
            TimeUnit::Microsecond => (1_000_000, 1_000),
            TimeUnit::Millisecond => (1_000, 1_000_000),
            TimeUnit::Second => (1, 1_000_000_000),
        };

        use chrono::naive::NaiveTime;
        let time = v.parse::<NaiveTime>()?;
        let timestamp = time.num_seconds_from_midnight() as i64 * seconds_factor
            + time.nanosecond() as i64 / nanoseconds_factor;

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
