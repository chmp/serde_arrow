use std::collections::BTreeMap;

use chrono::Timelike;

use crate::internal::{
    arrow::{Array, PrimitiveArray, TimeArray, TimeUnit},
    error::{Context, Error, Result},
    utils::{
        array_ext::{new_primitive_array, ArrayExt, ScalarArrayExt},
        btree_map,
    },
};

use super::simple_serializer::SimpleSerializer;

#[derive(Debug, Clone)]
pub struct TimeBuilder<I> {
    path: String,
    pub unit: TimeUnit,
    pub array: PrimitiveArray<I>,
}

impl<I: Default + 'static> TimeBuilder<I> {
    pub fn new(path: String, unit: TimeUnit, is_nullable: bool) -> Self {
        Self {
            path,
            unit,
            array: new_primitive_array(is_nullable),
        }
    }

    pub fn take(&mut self) -> Self {
        Self {
            path: self.path.clone(),
            unit: self.unit,
            array: self.array.take(),
        }
    }

    pub fn is_nullable(&self) -> bool {
        self.array.validity.is_some()
    }
}

impl TimeBuilder<i32> {
    pub fn into_array(self) -> Result<Array> {
        Ok(Array::Time32(TimeArray {
            unit: self.unit,
            validity: self.array.validity,
            values: self.array.values,
        }))
    }
}

impl TimeBuilder<i64> {
    pub fn into_array(self) -> Result<Array> {
        Ok(Array::Time64(TimeArray {
            unit: self.unit,
            validity: self.array.validity,
            values: self.array.values,
        }))
    }
}

impl<I> Context for TimeBuilder<I> {
    fn annotations(&self) -> BTreeMap<String, String> {
        btree_map!("field" => self.path.clone())
    }
}

impl<I> SimpleSerializer for TimeBuilder<I>
where
    I: TryFrom<i64> + TryFrom<i32> + Default + 'static,
    Error: From<<I as TryFrom<i32>>::Error>,
    Error: From<<I as TryFrom<i64>>::Error>,
{
    fn name(&self) -> &str {
        "Time64Builder"
    }

    fn annotate_error(&self, err: Error) -> Error {
        err.annotate_unannotated(|annotations| {
            annotations.insert(String::from("field"), self.path.clone());
        })
    }

    fn serialize_default(&mut self) -> Result<()> {
        self.array.push_scalar_default()
    }

    fn serialize_none(&mut self) -> Result<()> {
        self.array.push_scalar_none()
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

        self.array.push_scalar_value(timestamp.try_into()?)
    }

    fn serialize_i32(&mut self, v: i32) -> Result<()> {
        self.array.push_scalar_value(v.try_into()?)
    }

    fn serialize_i64(&mut self, v: i64) -> Result<()> {
        self.array.push_scalar_value(v.try_into()?)
    }
}
