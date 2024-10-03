use std::collections::BTreeMap;

use crate::internal::{
    arrow::{Array, PrimitiveArray, TimeArray, TimeUnit},
    chrono,
    error::{set_default, try_, Context, ContextSupport, Result},
    utils::array_ext::{new_primitive_array, ArrayExt, ScalarArrayExt},
};

use super::{array_builder::ArrayBuilder, simple_serializer::SimpleSerializer};

#[derive(Debug, Clone)]
pub struct DurationBuilder {
    path: String,
    pub unit: TimeUnit,
    pub array: PrimitiveArray<i64>,
}

impl DurationBuilder {
    pub fn new(path: String, unit: TimeUnit, is_nullable: bool) -> Self {
        Self {
            path,
            unit,
            array: new_primitive_array(is_nullable),
        }
    }

    pub fn take(&mut self) -> ArrayBuilder {
        ArrayBuilder::Duration(Self {
            path: self.path.clone(),
            unit: self.unit,
            array: self.array.take(),
        })
    }

    pub fn is_nullable(&self) -> bool {
        self.array.validity.is_some()
    }

    pub fn into_array(self) -> Result<Array> {
        Ok(Array::Duration(TimeArray {
            unit: self.unit,
            validity: self.array.validity,
            values: self.array.values,
        }))
    }
}

impl Context for DurationBuilder {
    fn annotate(&self, annotations: &mut BTreeMap<String, String>) {
        set_default(annotations, "field", &self.path);
        set_default(annotations, "data_type", "Duration(..)");
    }
}

impl SimpleSerializer for DurationBuilder {
    fn serialize_default(&mut self) -> Result<()> {
        try_(|| self.array.push_scalar_default()).ctx(self)
    }

    fn serialize_none(&mut self) -> Result<()> {
        try_(|| self.array.push_scalar_none()).ctx(self)
    }

    fn serialize_i8(&mut self, v: i8) -> Result<()> {
        try_(|| self.array.push_scalar_value(i64::from(v))).ctx(self)
    }

    fn serialize_i16(&mut self, v: i16) -> Result<()> {
        try_(|| self.array.push_scalar_value(i64::from(v))).ctx(self)
    }

    fn serialize_i32(&mut self, v: i32) -> Result<()> {
        try_(|| self.array.push_scalar_value(i64::from(v))).ctx(self)
    }

    fn serialize_i64(&mut self, v: i64) -> Result<()> {
        try_(|| self.array.push_scalar_value(v)).ctx(self)
    }

    fn serialize_u8(&mut self, v: u8) -> Result<()> {
        try_(|| self.array.push_scalar_value(i64::from(v))).ctx(self)
    }

    fn serialize_u16(&mut self, v: u16) -> Result<()> {
        try_(|| self.array.push_scalar_value(i64::from(v))).ctx(self)
    }

    fn serialize_u32(&mut self, v: u32) -> Result<()> {
        try_(|| self.array.push_scalar_value(i64::from(v))).ctx(self)
    }

    fn serialize_u64(&mut self, v: u64) -> Result<()> {
        try_(|| self.array.push_scalar_value(i64::try_from(v)?)).ctx(self)
    }

    fn serialize_str(&mut self, v: &str) -> Result<()> {
        try_(|| {
            let value = chrono::parse_span(v)?.to_arrow_duration(self.unit)?;
            self.array.push_scalar_value(value)
        })
        .ctx(self)
    }
}
