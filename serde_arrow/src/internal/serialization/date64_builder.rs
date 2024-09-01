use std::collections::BTreeMap;

use crate::internal::{
    arrow::{Array, PrimitiveArray, TimeUnit, TimestampArray},
    error::{fail, Context, ContextSupport, Result},
    utils::{
        array_ext::{new_primitive_array, ArrayExt, ScalarArrayExt},
        btree_map,
    },
};

use super::simple_serializer::SimpleSerializer;

#[derive(Debug, Clone)]
pub struct Date64Builder {
    path: String,
    pub meta: Option<(TimeUnit, Option<String>)>,
    pub utc: bool,
    pub array: PrimitiveArray<i64>,
}

impl Date64Builder {
    pub fn new(
        path: String,
        meta: Option<(TimeUnit, Option<String>)>,
        utc: bool,
        is_nullable: bool,
    ) -> Self {
        Self {
            path,
            meta,
            utc,
            array: new_primitive_array(is_nullable),
        }
    }

    pub fn take(&mut self) -> Self {
        Self {
            path: self.path.clone(),
            meta: self.meta.clone(),
            utc: self.utc,
            array: self.array.take(),
        }
    }

    pub fn is_nullable(&self) -> bool {
        self.array.validity.is_some()
    }

    pub fn into_array(self) -> Result<Array> {
        if let Some((unit, timezone)) = self.meta {
            Ok(Array::Timestamp(TimestampArray {
                unit,
                timezone,
                validity: self.array.validity,
                values: self.array.values,
            }))
        } else {
            Ok(Array::Date64(PrimitiveArray {
                validity: self.array.validity,
                values: self.array.values,
            }))
        }
    }
}

impl Date64Builder {
    fn parse_str_to_timestamp(&self, s: &str) -> Result<i64> {
        use chrono::{DateTime, NaiveDateTime, Utc};

        let date_time = if self.utc {
            s.parse::<DateTime<Utc>>()?
        } else {
            s.parse::<NaiveDateTime>()?.and_utc()
        };

        match self.meta.as_ref() {
            Some((TimeUnit::Nanosecond, _)) => match date_time.timestamp_nanos_opt() {
                Some(timestamp) => Ok(timestamp),
                _ => fail!(
                    concat!(
                        "Timestamp '{date_time}' cannot be converted to nanoseconds. ",
                        "The dates that can be represented as nanoseconds are between ",
                        "1677-09-21T00:12:44.0 and 2262-04-11T23:47:16.854775804.",
                    ),
                    date_time = date_time,
                ),
            },
            Some((TimeUnit::Microsecond, _)) => Ok(date_time.timestamp_micros()),
            Some((TimeUnit::Millisecond, _)) | None => Ok(date_time.timestamp_millis()),
            Some((TimeUnit::Second, _)) => Ok(date_time.timestamp()),
        }
    }
}

impl Context for Date64Builder {
    fn annotations(&self) -> BTreeMap<String, String> {
        btree_map!("field" => self.path.clone())
    }
}

impl SimpleSerializer for Date64Builder {
    fn serialize_default(&mut self) -> Result<()> {
        self.array.push_scalar_default().ctx(self)
    }

    fn serialize_none(&mut self) -> Result<()> {
        self.array.push_scalar_none().ctx(self)
    }

    fn serialize_str(&mut self, v: &str) -> Result<()> {
        let timestamp = self.parse_str_to_timestamp(v).ctx(self)?;
        self.array.push_scalar_value(timestamp)
    }

    fn serialize_i64(&mut self, v: i64) -> Result<()> {
        self.array.push_scalar_value(v).ctx(self)
    }
}
