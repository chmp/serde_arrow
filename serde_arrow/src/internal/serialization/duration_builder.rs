use std::collections::BTreeMap;

use crate::internal::{
    arrow::{Array, PrimitiveArray, TimeArray, TimeUnit},
    error::{Context, ContextSupport, Result},
    utils::{
        array_ext::{new_primitive_array, ArrayExt, ScalarArrayExt},
        btree_map,
    },
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
    fn annotations(&self) -> BTreeMap<String, String> {
        btree_map!("field" => self.path.clone(), "data_type" => "Duration(..)")
    }
}

impl SimpleSerializer for DurationBuilder {
    fn serialize_default(&mut self) -> Result<()> {
        self.array.push_scalar_default().ctx(self)
    }

    fn serialize_none(&mut self) -> Result<()> {
        self.array.push_scalar_none().ctx(self)
    }

    fn serialize_i8(&mut self, v: i8) -> Result<()> {
        self.array.push_scalar_value(i64::from(v)).ctx(self)
    }

    fn serialize_i16(&mut self, v: i16) -> Result<()> {
        self.array.push_scalar_value(i64::from(v)).ctx(self)
    }

    fn serialize_i32(&mut self, v: i32) -> Result<()> {
        self.array.push_scalar_value(i64::from(v)).ctx(self)
    }

    fn serialize_i64(&mut self, v: i64) -> Result<()> {
        self.array.push_scalar_value(v).ctx(self)
    }

    fn serialize_u8(&mut self, v: u8) -> Result<()> {
        self.array.push_scalar_value(i64::from(v)).ctx(self)
    }

    fn serialize_u16(&mut self, v: u16) -> Result<()> {
        self.array.push_scalar_value(i64::from(v)).ctx(self)
    }

    fn serialize_u32(&mut self, v: u32) -> Result<()> {
        self.array.push_scalar_value(i64::from(v)).ctx(self)
    }

    fn serialize_u64(&mut self, v: u64) -> Result<()> {
        self.array
            .push_scalar_value(i64::try_from(v).ctx(self)?)
            .ctx(self)
    }
}
