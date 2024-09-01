use std::collections::BTreeMap;

use chrono::{NaiveDate, NaiveDateTime};

use crate::internal::{
    arrow::{Array, PrimitiveArray},
    error::{Context, ContextSupport, Result},
    utils::{
        array_ext::{new_primitive_array, ArrayExt, ScalarArrayExt},
        btree_map,
    },
};

use super::{array_builder::ArrayBuilder, simple_serializer::SimpleSerializer};

#[derive(Debug, Clone)]
pub struct Date32Builder {
    path: String,
    array: PrimitiveArray<i32>,
}

impl Date32Builder {
    pub fn new(path: String, is_nullable: bool) -> Self {
        Self {
            path,
            array: new_primitive_array(is_nullable),
        }
    }

    pub fn take(&mut self) -> ArrayBuilder {
        ArrayBuilder::Date32(Self {
            path: self.path.clone(),
            array: self.array.take(),
        })
    }

    pub fn is_nullable(&self) -> bool {
        self.array.validity.is_some()
    }

    pub fn into_array(self) -> Result<Array> {
        Ok(Array::Date32(self.array))
    }

    fn parse_str_to_days_since_epoch(&self, s: &str) -> Result<i32> {
        const UNIX_EPOCH: NaiveDate = NaiveDateTime::UNIX_EPOCH.date();

        let date = s.parse::<NaiveDate>()?;
        let duration_since_epoch = date.signed_duration_since(UNIX_EPOCH);
        let days_since_epoch = duration_since_epoch.num_days().try_into()?;

        Ok(days_since_epoch)
    }
}

impl Context for Date32Builder {
    fn annotations(&self) -> BTreeMap<String, String> {
        btree_map!("field" => self.path.clone())
    }
}

impl SimpleSerializer for Date32Builder {
    fn serialize_default(&mut self) -> Result<()> {
        self.array.push_scalar_default().ctx(self)
    }

    fn serialize_none(&mut self) -> Result<()> {
        self.array.push_scalar_none().ctx(self)
    }

    fn serialize_str(&mut self, v: &str) -> Result<()> {
        let days_since_epoch = self.parse_str_to_days_since_epoch(v).ctx(self)?;
        self.array.push_scalar_value(days_since_epoch).ctx(self)
    }

    fn serialize_i32(&mut self, v: i32) -> Result<()> {
        self.array.push_scalar_value(v).ctx(self)
    }
}
