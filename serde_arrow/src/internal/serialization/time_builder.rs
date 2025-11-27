use std::collections::{BTreeMap, HashMap};

use chrono::Timelike;
use marrow::{
    array::{Array, PrimitiveArray, TimeArray},
    datatypes::{FieldMeta, TimeUnit},
};
use serde::{Serialize, Serializer};

use crate::internal::{
    error::{set_default, try_, Context, ContextSupport, Result},
    serialization::utils::impl_serializer,
    utils::{
        array_ext::{ArrayExt, ScalarArrayExt},
        NamedType,
    },
};

use super::array_builder::ArrayBuilder;

pub trait TimeType: NamedType + Default + 'static {
    const ARRAY_BUILDER_VARIANT: fn(TimeBuilder<Self>) -> ArrayBuilder;
    const ARRAY_VARIANT: fn(TimeArray<Self>) -> Array;

    fn from_i32(v: i32) -> Result<Self>;
    fn from_i64(v: i64) -> Result<Self>;
}

macro_rules! impl_time_type {
    ($ty:ty, $variant:ident) => {
        impl TimeType for $ty {
            const ARRAY_BUILDER_VARIANT: fn(TimeBuilder<Self>) -> ArrayBuilder =
                ArrayBuilder::$variant;
            const ARRAY_VARIANT: fn(TimeArray<Self>) -> Array = Array::$variant;

            fn from_i32(v: i32) -> Result<Self> {
                Ok(Self::try_from(v)?)
            }

            fn from_i64(v: i64) -> Result<Self> {
                Ok(Self::try_from(v)?)
            }
        }
    };
}

impl_time_type!(i32, Time32);
impl_time_type!(i64, Time64);

#[derive(Debug, Clone)]
pub struct TimeBuilder<I> {
    pub name: String,
    pub unit: TimeUnit,
    pub array: PrimitiveArray<I>,
    metadata: HashMap<String, String>,
}

impl<I: TimeType> TimeBuilder<I> {
    pub fn new(
        name: String,
        unit: TimeUnit,
        is_nullable: bool,
        metadata: HashMap<String, String>,
    ) -> Self {
        Self {
            name,
            unit,
            array: PrimitiveArray::new(is_nullable),
            metadata,
        }
    }

    pub fn take_self(&mut self) -> Self {
        Self {
            name: self.name.clone(),
            metadata: self.metadata.clone(),
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

    pub fn serialize_value<V: Serialize>(&mut self, value: V) -> Result<()> {
        value.serialize(&mut *self).ctx(self)
    }

    pub fn take(&mut self) -> ArrayBuilder {
        I::ARRAY_BUILDER_VARIANT(self.take_self())
    }

    pub fn into_array_and_field_meta(self) -> Result<(Array, FieldMeta)> {
        let meta = FieldMeta {
            name: self.name,
            metadata: self.metadata,
            nullable: self.array.is_nullable(),
        };
        let array = I::ARRAY_VARIANT(TimeArray {
            unit: self.unit,
            validity: self.array.validity,
            values: self.array.values,
        });
        Ok((array, meta))
    }
}

impl<I: TimeType> Context for TimeBuilder<I> {
    fn annotate(&self, annotations: &mut BTreeMap<String, String>) {
        set_default(annotations, "field", &self.name);
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

impl<'a, I: TimeType> Serializer for &'a mut TimeBuilder<I> {
    impl_serializer!(
        'a, TimeBuilder;
        override serialize_none,
        override serialize_str,
        override serialize_i32,
        override serialize_i64,
    );

    fn serialize_none(self) -> Result<()> {
        self.array.push_scalar_none()
    }

    fn serialize_str(self, v: &str) -> Result<()> {
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

        self.array.push_scalar_value(I::from_i64(timestamp)?)
    }

    fn serialize_i32(self, v: i32) -> Result<()> {
        self.array.push_scalar_value(I::from_i32(v)?)
    }

    fn serialize_i64(self, v: i64) -> Result<()> {
        self.array.push_scalar_value(I::from_i64(v)?)
    }
}
