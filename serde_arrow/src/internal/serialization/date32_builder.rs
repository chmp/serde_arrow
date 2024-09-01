use std::collections::BTreeMap;

use chrono::{NaiveDate, NaiveDateTime};

use crate::internal::{
    arrow::{Array, PrimitiveArray},
    error::{Context, Error, Result},
    utils::{
        array_ext::{new_primitive_array, ArrayExt, ScalarArrayExt},
        btree_map,
    },
};

use super::simple_serializer::SimpleSerializer;

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

    pub fn take(&mut self) -> Self {
        Self {
            path: self.path.clone(),
            array: self.array.take(),
        }
    }

    pub fn is_nullable(&self) -> bool {
        self.array.validity.is_some()
    }

    pub fn into_array(self) -> Result<Array> {
        Ok(Array::Date32(self.array))
    }
}

impl Context for Date32Builder {
    fn annotations(&self) -> BTreeMap<String, String> {
        btree_map!("field" => self.path.clone())
    }
}

impl SimpleSerializer for Date32Builder {
    fn name(&self) -> &str {
        "Date32Builder"
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
        const UNIX_EPOCH: NaiveDate = NaiveDateTime::UNIX_EPOCH.date();

        let date = v.parse::<NaiveDate>()?;
        let duration_since_epoch = date.signed_duration_since(UNIX_EPOCH);
        let days_since_epoch = duration_since_epoch.num_days().try_into()?;

        self.array.push_scalar_value(days_since_epoch)
    }

    fn serialize_i32(&mut self, v: i32) -> Result<()> {
        self.array.push_scalar_value(v)
    }
}
