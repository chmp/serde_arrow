use std::collections::BTreeMap;

use marrow::array::{Array, DecimalArray, PrimitiveArray};

use crate::internal::{
    error::{set_default, try_, Context, ContextSupport, Result},
    serialization::utils::impl_serializer,
    utils::{
        array_ext::{ArrayExt, ScalarArrayExt},
        decimal::{self, DecimalParser},
    },
};

use super::array_builder::ArrayBuilder;

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
            array: PrimitiveArray::new(is_nullable),
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
        self.array.is_nullable()
    }

    pub fn into_array(self) -> Result<Array> {
        Ok(Array::Decimal128(DecimalArray {
            precision: self.precision,
            scale: self.scale,
            validity: self.array.validity,
            values: self.array.values,
        }))
    }

    pub fn reserve(&mut self, additional: usize) {
        self.array.reserve(additional);
    }

    pub fn serialize_default_value(&mut self) -> Result<()> {
        try_(|| self.array.push_scalar_default()).ctx(self)
    }
}

impl Context for DecimalBuilder {
    fn annotate(&self, annotations: &mut BTreeMap<String, String>) {
        set_default(annotations, "filed", &self.path);
        set_default(annotations, "data_type", "Decimal128(..)");
    }
}

impl<'a> serde::Serializer for &'a mut DecimalBuilder {
    impl_serializer!(
        'a, DecimalBuilder;
        override serialize_none,
        override serialize_f32,
        override serialize_f64,
        override serialize_str,
    );

    fn serialize_none(self) -> Result<()> {
        try_(|| self.array.push_scalar_none()).ctx(self)
    }

    fn serialize_f32(self, v: f32) -> Result<()> {
        try_(|| self.array.push_scalar_value((v * self.f32_factor) as i128)).ctx(self)
    }

    fn serialize_f64(self, v: f64) -> Result<()> {
        try_(|| self.array.push_scalar_value((v * self.f64_factor) as i128)).ctx(self)
    }

    fn serialize_str(self, v: &str) -> Result<()> {
        try_(|| {
            let mut parse_buffer = [0; decimal::BUFFER_SIZE_I128];
            let val = self
                .parser
                .parse_decimal128(&mut parse_buffer, v.as_bytes())?;

            self.array.push_scalar_value(val)
        })
        .ctx(self)
    }
}
