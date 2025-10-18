use std::collections::BTreeMap;

use marrow::{
    array::{Array, PrimitiveArray, TimestampArray},
    datatypes::TimeUnit,
};

use crate::internal::{
    error::{fail, set_default, try_, Context, ContextSupport, Result},
    serialization::utils::impl_serializer,
    utils::array_ext::{ArrayExt, ScalarArrayExt},
};

use super::{array_builder::ArrayBuilder, simple_serializer::SimpleSerializer};

#[derive(Debug, Clone)]
pub struct TimestampBuilder {
    path: String,
    pub unit: TimeUnit,
    pub timezone: Option<String>,
    pub utc: bool,
    pub array: PrimitiveArray<i64>,
}

impl TimestampBuilder {
    pub fn new(
        path: String,
        unit: TimeUnit,
        timezone: Option<String>,
        is_nullable: bool,
    ) -> Result<Self> {
        Ok(Self {
            utc: is_utc_tz(timezone.as_deref())?,
            path,
            unit,
            timezone,
            array: PrimitiveArray::new(is_nullable),
        })
    }

    pub fn take(&mut self) -> ArrayBuilder {
        ArrayBuilder::Timestamp(Self {
            path: self.path.clone(),
            unit: self.unit,
            timezone: self.timezone.clone(),
            utc: self.utc,
            array: self.array.take(),
        })
    }

    pub fn is_nullable(&self) -> bool {
        self.array.is_nullable()
    }

    pub fn into_array(self) -> Result<Array> {
        Ok(Array::Timestamp(TimestampArray {
            unit: self.unit,
            timezone: self.timezone,
            validity: self.array.validity,
            values: self.array.values,
        }))
    }

    pub fn reserve(&mut self, additional: usize) {
        self.array.reserve(additional);
    }

    pub fn serialize_default_value(&mut self) -> Result<()> {
        try_(|| self.array.push_scalar_default()).ctx(self)
    }
}

fn is_utc_tz(tz: Option<&str>) -> Result<bool> {
    match tz {
        None => Ok(false),
        Some(tz) if tz.to_uppercase() == "UTC" => Ok(true),
        Some(tz) => fail!("Timezone {tz} is not supported"),
    }
}

impl TimestampBuilder {
    fn parse_str_to_timestamp(&self, s: &str) -> Result<i64> {
        use chrono::{DateTime, NaiveDateTime, Utc};

        let date_time = if self.utc {
            s.parse::<DateTime<Utc>>()?
        } else {
            s.parse::<NaiveDateTime>()?.and_utc()
        };

        match self.unit {
            TimeUnit::Nanosecond => match date_time.timestamp_nanos_opt() {
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
            TimeUnit::Microsecond => Ok(date_time.timestamp_micros()),
            TimeUnit::Millisecond => Ok(date_time.timestamp_millis()),
            TimeUnit::Second => Ok(date_time.timestamp()),
        }
    }
}

impl Context for TimestampBuilder {
    fn annotate(&self, annotations: &mut BTreeMap<String, String>) {
        set_default(annotations, "field", &self.path);
        set_default(annotations, "data_type", "Timestamp(..)");
    }
}

impl SimpleSerializer for TimestampBuilder {
    fn serialize_default(&mut self) -> Result<()> {
        self.serialize_default_value()
    }

    fn serialize_none(&mut self) -> Result<()> {
        try_(|| self.array.push_scalar_none()).ctx(self)
    }

    fn serialize_str(&mut self, v: &str) -> Result<()> {
        try_(|| {
            let timestamp = self.parse_str_to_timestamp(v)?;
            self.array.push_scalar_value(timestamp)
        })
        .ctx(self)
    }

    fn serialize_i64(&mut self, v: i64) -> Result<()> {
        try_(|| self.array.push_scalar_value(v)).ctx(self)
    }
}

impl<'a> serde::Serializer for &'a mut TimestampBuilder {
    impl_serializer!(
        'a, TimestampBuilder;
        override serialize_none,
        override serialize_str,
        override serialize_i64,
    );

    fn serialize_none(self) -> Result<()> {
        try_(|| self.array.push_scalar_none()).ctx(self)
    }

    fn serialize_str(self, v: &str) -> Result<()> {
        try_(|| {
            let timestamp = self.parse_str_to_timestamp(v)?;
            self.array.push_scalar_value(timestamp)
        })
        .ctx(self)
    }

    fn serialize_i64(self, v: i64) -> Result<()> {
        try_(|| self.array.push_scalar_value(v)).ctx(self)
    }
}
