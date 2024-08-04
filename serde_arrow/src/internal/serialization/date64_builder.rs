use crate::internal::{
    arrow::{Array, PrimitiveArray, TimeUnit, TimestampArray},
    error::{fail, Result},
    schema::{GenericDataType, GenericField},
};

use super::{
    array_ext::{new_primitive_array, ArrayExt, ScalarArrayExt},
    utils::SimpleSerializer,
};

#[derive(Debug, Clone)]
pub struct Date64Builder {
    pub field: GenericField,
    pub utc: bool,
    pub array: PrimitiveArray<i64>,
}

impl Date64Builder {
    pub fn new(field: GenericField, utc: bool, is_nullable: bool) -> Self {
        Self {
            field,
            utc,
            array: new_primitive_array(is_nullable),
        }
    }

    pub fn take(&mut self) -> Self {
        Self {
            field: self.field.clone(),
            utc: self.utc,
            array: self.array.take(),
        }
    }

    pub fn is_nullable(&self) -> bool {
        self.array.validity.is_some()
    }

    // TODO: fix this
    pub fn into_array(self) -> Result<Array> {
        if let GenericDataType::Timestamp(unit, timezone) = self.field.data_type {
            Ok(Array::Timestamp(TimestampArray {
                unit,
                timezone,
                validity: self.array.validity,
                values: self.array.values,
            }))
        } else {
            // TOOD: check data type
            Ok(Array::Date64(PrimitiveArray {
                validity: self.array.validity,
                values: self.array.values,
            }))
        }
    }
}

impl SimpleSerializer for Date64Builder {
    fn name(&self) -> &str {
        "Date64Builder"
    }

    fn serialize_default(&mut self) -> Result<()> {
        self.array.push_scalar_default()
    }

    fn serialize_none(&mut self) -> Result<()> {
        self.array.push_scalar_none()
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
            GenericDataType::Timestamp(TimeUnit::Nanosecond, _) => {
                match date_time.timestamp_nanos_opt() {
                    Some(timestamp) => timestamp,
                    _ => fail!(
                        concat!(
                            "Timestamp '{date_time}' cannot be converted to nanoseconds. ",
                            "The dates that can be represented as nanoseconds are between ",
                            "1677-09-21T00:12:44.0 and 2262-04-11T23:47:16.854775804.",
                        ),
                        date_time = date_time,
                    ),
                }
            }
            GenericDataType::Timestamp(TimeUnit::Microsecond, _) => date_time.timestamp_micros(),
            GenericDataType::Timestamp(TimeUnit::Millisecond, _) => date_time.timestamp_millis(),
            GenericDataType::Timestamp(TimeUnit::Second, _) => date_time.timestamp(),
            _ => date_time.timestamp_millis(),
        };

        self.array.push_scalar_value(timestamp)
    }

    fn serialize_i64(&mut self, v: i64) -> Result<()> {
        self.array.push_scalar_value(v)
    }
}
