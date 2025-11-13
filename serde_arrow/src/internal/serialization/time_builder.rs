use std::collections::BTreeMap;

use chrono::Timelike;
use marrow::{
    array::{Array, PrimitiveArray, TimeArray},
    datatypes::TimeUnit,
};

use crate::internal::{
    error::{set_default, try_, Context, ContextSupport, Error, Result},
    serialization::utils::impl_serializer,
    utils::{
        array_ext::{ArrayExt, ScalarArrayExt},
        NamedType,
    },
};

use super::array_builder::ArrayBuilder;

#[derive(Debug, Clone)]
pub struct TimeBuilder<I> {
    path: String,
    pub unit: TimeUnit,
    pub array: PrimitiveArray<I>,
}

impl<I: Default + NamedType + 'static> TimeBuilder<I> {
    pub fn new(path: String, unit: TimeUnit, is_nullable: bool) -> Self {
        Self {
            path,
            unit,
            array: PrimitiveArray::new(is_nullable),
        }
    }

    pub fn take_self(&mut self) -> Self {
        Self {
            path: self.path.clone(),
            unit: self.unit,
            array: self.array.take(),
        }
    }

    pub fn is_nullable(&self) -> bool {
        self.array.is_nullable()
    }

    pub fn reserve(&mut self, additional: usize) {
        self.array.reserve(additional);
    }

    pub fn serialize_default_value(&mut self) -> Result<()> {
        try_(|| self.array.push_scalar_default()).ctx(self)
    }
}

impl TimeBuilder<i32> {
    pub fn take(&mut self) -> ArrayBuilder {
        ArrayBuilder::Time32(self.take_self())
    }

    pub fn into_array(self) -> Result<Array> {
        Ok(Array::Time32(TimeArray {
            unit: self.unit,
            validity: self.array.validity,
            values: self.array.values,
        }))
    }
}

impl TimeBuilder<i64> {
    pub fn take(&mut self) -> ArrayBuilder {
        ArrayBuilder::Time64(self.take_self())
    }

    pub fn into_array(self) -> Result<Array> {
        Ok(Array::Time64(TimeArray {
            unit: self.unit,
            validity: self.array.validity,
            values: self.array.values,
        }))
    }
}

impl<I: NamedType> Context for TimeBuilder<I> {
    fn annotate(&self, annotations: &mut BTreeMap<String, String>) {
        set_default(annotations, "field", &self.path);
        set_default(
            annotations,
            "data_type",
            match I::NAME {
                "i32" => "Time32",
                "i64" => "Time64",
                _ => "<unknown>",
            },
        );
    }
}

impl<'a, I> serde::Serializer for &'a mut TimeBuilder<I>
where
    I: NamedType + TryFrom<i64> + TryFrom<i32> + Default + 'static,
    Error: From<<I as TryFrom<i32>>::Error>,
    Error: From<<I as TryFrom<i64>>::Error>,
{
    impl_serializer!(
        'a, TimeBuilder;
        override serialize_none,
        override serialize_str,
        override serialize_i32,
        override serialize_i64,
    );

    fn serialize_none(self) -> Result<()> {
        try_(|| self.array.push_scalar_none()).ctx(self)
    }

    fn serialize_str(self, v: &str) -> Result<()> {
        try_(|| {
            let (seconds_factor, nanoseconds_factor) = match self.unit {
                TimeUnit::Nanosecond => (1_000_000_000, 1),
                TimeUnit::Microsecond => (1_000_000, 1_000),
                TimeUnit::Millisecond => (1_000, 1_000_000),
                TimeUnit::Second => (1, 1_000_000_000),
            };

            use chrono::naive::NaiveTime;
            let time = v.parse::<NaiveTime>()?;
            let timestamp = i64::from(time.num_seconds_from_midnight()) * seconds_factor
                + i64::from(time.nanosecond()) / nanoseconds_factor;

            self.array.push_scalar_value(timestamp.try_into()?)
        })
        .ctx(self)
    }

    fn serialize_i32(self, v: i32) -> Result<()> {
        try_(|| self.array.push_scalar_value(v.try_into()?)).ctx(self)
    }

    fn serialize_i64(self, v: i64) -> Result<()> {
        try_(|| self.array.push_scalar_value(v.try_into()?)).ctx(self)
    }
}
