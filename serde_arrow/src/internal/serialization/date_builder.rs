use std::collections::{BTreeMap, HashMap};

use chrono::{NaiveDate, NaiveDateTime};
use marrow::{
    array::{Array, PrimitiveArray},
    datatypes::FieldMeta,
};

use crate::internal::{
    error::{fail, set_default, try_, Context, ContextSupport, Result},
    serialization::utils::impl_serializer,
    utils::array_ext::{ArrayExt, ScalarArrayExt},
};

use super::array_builder::ArrayBuilder;

pub trait DatePrimitive:
    TryFrom<i32>
    + TryFrom<i64>
    + Copy
    + std::fmt::Display
    + std::default::Default
    + std::ops::Mul<Self, Output = Self>
    + Sized
    + 'static
{
    const NAME: &'static str;
    const DATA_TYPE_NAME: &'static str;
    const DAY_TO_VALUE_FACTOR: Self;
    const ARRAY_BUILDER_VARIANT: fn(DateBuilder<Self>) -> ArrayBuilder;
    const ARRAY_VARIANT: fn(PrimitiveArray<Self>) -> Array;
}

impl DatePrimitive for i32 {
    const NAME: &'static str = "i32";
    const DATA_TYPE_NAME: &'static str = "Date32";
    const DAY_TO_VALUE_FACTOR: Self = 1;
    const ARRAY_BUILDER_VARIANT: fn(DateBuilder<Self>) -> ArrayBuilder = ArrayBuilder::Date32;
    const ARRAY_VARIANT: fn(PrimitiveArray<Self>) -> Array = Array::Date32;
}

impl DatePrimitive for i64 {
    const NAME: &'static str = "i64";
    const DATA_TYPE_NAME: &'static str = "Date64";
    const DAY_TO_VALUE_FACTOR: Self = 86_400_000;
    const ARRAY_BUILDER_VARIANT: fn(DateBuilder<Self>) -> ArrayBuilder = ArrayBuilder::Date64;
    const ARRAY_VARIANT: fn(PrimitiveArray<Self>) -> Array = Array::Date64;
}

#[derive(Debug, Clone)]
pub struct DateBuilder<I: DatePrimitive> {
    name: String,
    array: PrimitiveArray<I>,
    metadata: HashMap<String, String>,
}

impl<I: DatePrimitive> DateBuilder<I> {
    pub fn new(name: String, is_nullable: bool, metadata: HashMap<String, String>) -> Self {
        Self {
            name,
            array: PrimitiveArray::new(is_nullable),
            metadata,
        }
    }

    pub fn take(&mut self) -> ArrayBuilder {
        I::ARRAY_BUILDER_VARIANT(Self {
            name: self.name.clone(),
            metadata: self.metadata.clone(),
            array: self.array.take(),
        })
    }

    pub fn is_nullable(&self) -> bool {
        self.array.is_nullable()
    }

    pub fn into_array(self) -> Result<Array> {
        Ok(I::ARRAY_VARIANT(self.array))
    }

    pub fn into_array_and_field_meta(self) -> Result<(Array, FieldMeta)> {
        let meta = FieldMeta {
            name: self.name,
            metadata: self.metadata,
            nullable: self.array.is_nullable(),
        };
        Ok((I::ARRAY_VARIANT(self.array), meta))
    }

    pub fn reserve(&mut self, additional: usize) {
        self.array.reserve(additional);
    }

    fn parse_str_to_days_since_epoch(&self, s: &str) -> Result<I> {
        #[allow(deprecated)]
        const UNIX_EPOCH: NaiveDate = NaiveDateTime::UNIX_EPOCH.date();

        let date = s.parse::<NaiveDate>()?;
        let duration_since_epoch = date.signed_duration_since(UNIX_EPOCH).num_days();
        let Ok(days_since_epoch) = I::try_from(duration_since_epoch) else {
            fail!("cannot convert {duration_since_epoch} to {I}", I = I::NAME);
        };

        Ok(days_since_epoch * I::DAY_TO_VALUE_FACTOR)
    }

    pub fn serialize_default_value(&mut self) -> Result<()> {
        try_(|| self.array.push_scalar_default()).ctx(self)
    }
}

impl<I: DatePrimitive> Context for DateBuilder<I> {
    fn annotate(&self, annotations: &mut BTreeMap<String, String>) {
        set_default(annotations, "field", &self.name);
        set_default(annotations, "data_type", I::DATA_TYPE_NAME);
    }
}

impl<'a, D: DatePrimitive> serde::Serializer for &'a mut DateBuilder<D> {
    impl_serializer!(
        'a, DateBuilder;
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
            let days_since_epoch = self.parse_str_to_days_since_epoch(v)?;
            self.array.push_scalar_value(days_since_epoch)
        })
        .ctx(self)
    }

    fn serialize_i32(self, v: i32) -> Result<()> {
        try_(|| {
            let Ok(v) = D::try_from(v) else {
                fail!("cannot convert {v} to {D}", D = D::NAME);
            };
            self.array.push_scalar_value(v)
        })
        .ctx(self)
    }

    fn serialize_i64(self, v: i64) -> Result<()> {
        try_(|| {
            let Ok(v) = D::try_from(v) else {
                fail!("cannot convert {v} to {D}", D = D::NAME);
            };
            self.array.push_scalar_value(v)
        })
        .ctx(self)
    }
}
