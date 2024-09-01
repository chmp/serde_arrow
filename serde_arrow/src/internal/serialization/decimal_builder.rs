use std::collections::BTreeMap;

use crate::internal::{
    arrow::{Array, DecimalArray, PrimitiveArray},
    error::{Context, ContextSupport, Result},
    utils::{
        array_ext::{new_primitive_array, ArrayExt, ScalarArrayExt},
        btree_map,
        decimal::{self, DecimalParser},
    },
};

use super::{array_builder::ArrayBuilder, simple_serializer::SimpleSerializer};

#[derive(Debug, Clone)]
pub struct DecimalBuilder {
    path: String,
    pub precision: u8,
    pub scale: i8,
    pub f32_factor: f32,
    pub f64_factor: f64,
    pub parser: DecimalParser,
    pub array: PrimitiveArray<i128>,
}

impl DecimalBuilder {
    pub fn new(path: String, precision: u8, scale: i8, is_nullable: bool) -> Self {
        Self {
            path,
            precision,
            scale,
            f32_factor: (10.0_f32).powi(scale as i32),
            f64_factor: (10.0_f64).powi(scale as i32),
            parser: DecimalParser::new(precision, scale, true),
            array: new_primitive_array(is_nullable),
        }
    }

    pub fn take(&mut self) -> ArrayBuilder {
        ArrayBuilder::Decimal128(Self {
            path: self.path.clone(),
            precision: self.precision,
            scale: self.scale,
            f32_factor: self.f32_factor,
            f64_factor: self.f64_factor,
            parser: self.parser,
            array: self.array.take(),
        })
    }

    pub fn is_nullable(&self) -> bool {
        self.array.validity.is_some()
    }

    pub fn into_array(self) -> Result<Array> {
        Ok(Array::Decimal128(DecimalArray {
            precision: self.precision,
            scale: self.scale,
            validity: self.array.validity,
            values: self.array.values,
        }))
    }
}

impl Context for DecimalBuilder {
    fn annotations(&self) -> BTreeMap<String, String> {
        btree_map!("field" => self.path.clone())
    }
}

impl SimpleSerializer for DecimalBuilder {
    fn serialize_default(&mut self) -> Result<()> {
        self.array.push_scalar_default().ctx(self)
    }

    fn serialize_none(&mut self) -> Result<()> {
        self.array.push_scalar_none().ctx(self)
    }

    fn serialize_f32(&mut self, v: f32) -> Result<()> {
        self.array
            .push_scalar_value((v * self.f32_factor) as i128)
            .ctx(self)
    }

    fn serialize_f64(&mut self, v: f64) -> Result<()> {
        self.array
            .push_scalar_value((v * self.f64_factor) as i128)
            .ctx(self)
    }

    fn serialize_str(&mut self, v: &str) -> Result<()> {
        let mut parse_buffer = [0; decimal::BUFFER_SIZE_I128];
        let val = self
            .parser
            .parse_decimal128(&mut parse_buffer, v.as_bytes())
            .ctx(self)?;

        self.array.push_scalar_value(val).ctx(self)
    }
}
