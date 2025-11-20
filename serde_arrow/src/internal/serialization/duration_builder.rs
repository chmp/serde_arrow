use std::collections::{BTreeMap, HashMap};

use marrow::{
    array::{Array, PrimitiveArray, TimeArray},
    datatypes::{FieldMeta, TimeUnit},
};

use crate::internal::{
    chrono,
    error::{set_default, try_, Context, ContextSupport, Result},
    serialization::utils::impl_serializer,
    utils::array_ext::{ArrayExt, ScalarArrayExt},
};

use super::array_builder::ArrayBuilder;

#[derive(Debug, Clone)]
pub struct DurationBuilder {
    name: String,
    pub unit: TimeUnit,
    pub array: PrimitiveArray<i64>,
    metadata: HashMap<String, String>,
}

impl DurationBuilder {
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

    pub fn take(&mut self) -> ArrayBuilder {
        ArrayBuilder::Duration(Self {
            name: self.name.clone(),
            metadata: self.metadata.clone(),
            unit: self.unit,
            array: self.array.take(),
        })
    }

    pub fn is_nullable(&self) -> bool {
        self.array.is_nullable()
    }

    pub fn into_array(self) -> Result<Array> {
        Ok(Array::Duration(TimeArray {
            unit: self.unit,
            validity: self.array.validity,
            values: self.array.values,
        }))
    }

    pub fn into_array_and_field_meta(self) -> Result<(Array, FieldMeta)> {
        let meta = FieldMeta {
            name: self.name,
            metadata: self.metadata,
            nullable: self.array.is_nullable(),
        };
        let array = Array::Duration(TimeArray {
            unit: self.unit,
            validity: self.array.validity,
            values: self.array.values,
        });
        Ok((array, meta))
    }

    pub fn reserve(&mut self, additional: usize) {
        self.array.reserve(additional);
    }

    pub fn serialize_default_value(&mut self) -> Result<()> {
        try_(|| self.array.push_scalar_default()).ctx(self)
    }
}

impl Context for DurationBuilder {
    fn annotate(&self, annotations: &mut BTreeMap<String, String>) {
        set_default(annotations, "field", &self.name);
        set_default(annotations, "data_type", "Duration(..)");
    }
}

impl<'a> serde::Serializer for &'a mut DurationBuilder {
    impl_serializer!(
        'a, DurationBuilder;
        override serialize_none,
        override serialize_i8,
        override serialize_i16,
        override serialize_i32,
        override serialize_i64,
        override serialize_u8,
        override serialize_u16,
        override serialize_u32,
        override serialize_u64,
        override serialize_str,
    );

    fn serialize_none(self) -> Result<()> {
        try_(|| self.array.push_scalar_none()).ctx(self)
    }

    fn serialize_i8(self, v: i8) -> Result<()> {
        try_(|| self.array.push_scalar_value(i64::from(v))).ctx(self)
    }

    fn serialize_i16(self, v: i16) -> Result<()> {
        try_(|| self.array.push_scalar_value(i64::from(v))).ctx(self)
    }

    fn serialize_i32(self, v: i32) -> Result<()> {
        try_(|| self.array.push_scalar_value(i64::from(v))).ctx(self)
    }

    fn serialize_i64(self, v: i64) -> Result<()> {
        try_(|| self.array.push_scalar_value(v)).ctx(self)
    }

    fn serialize_u8(self, v: u8) -> Result<()> {
        try_(|| self.array.push_scalar_value(i64::from(v))).ctx(self)
    }

    fn serialize_u16(self, v: u16) -> Result<()> {
        try_(|| self.array.push_scalar_value(i64::from(v))).ctx(self)
    }

    fn serialize_u32(self, v: u32) -> Result<()> {
        try_(|| self.array.push_scalar_value(i64::from(v))).ctx(self)
    }

    fn serialize_u64(self, v: u64) -> Result<()> {
        try_(|| self.array.push_scalar_value(i64::try_from(v)?)).ctx(self)
    }

    fn serialize_str(self, v: &str) -> Result<()> {
        try_(|| {
            let value = chrono::parse_span(v)?.to_arrow_duration(self.unit)?;
            self.array.push_scalar_value(value)
        })
        .ctx(self)
    }
}
