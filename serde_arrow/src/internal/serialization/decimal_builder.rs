use std::collections::{BTreeMap, HashMap};

use marrow::{
    array::{Array, DecimalArray, PrimitiveArray},
    datatypes::FieldMeta,
    types::i256,
};
use serde::{Serialize, Serializer};

use crate::internal::{
    error::{fail, set_default, try_, Context, ContextSupport, Result},
    serialization::utils::impl_serializer,
    utils::{
        array_ext::{ArrayExt, ScalarArrayExt},
        decimal::{self, DecimalParser},
    },
};

use super::array_builder::ArrayBuilder;

pub trait DecimalPrimitive: Copy + Default + 'static {
    const DATA_TYPE_NAME: &'static str;
    const ARRAY_BUILDER_VARIANT: fn(DecimalBuilder<Self>) -> ArrayBuilder;
    const ARRAY_VARIANT: fn(DecimalArray<Self>) -> Array;

    fn parse(parser: DecimalParser, s: &str) -> Result<Self>;
    /// Convert an `f32` that is not yet multiplied by `10^scale`
    fn from_f32(builder: &DecimalBuilder<Self>, value: f32) -> Result<Self>;
    fn from_scaled_f64(value: f64) -> Result<Self>;
}

impl DecimalPrimitive for i128 {
    const DATA_TYPE_NAME: &'static str = "Decimal128";
    const ARRAY_BUILDER_VARIANT: fn(DecimalBuilder<Self>) -> ArrayBuilder =
        ArrayBuilder::Decimal128;
    const ARRAY_VARIANT: fn(DecimalArray<Self>) -> Array = Array::Decimal128;

    fn parse(parser: DecimalParser, s: &str) -> Result<Self> {
        let mut parse_buffer = [0; decimal::BUFFER_SIZE_I128];
        parser.parse_decimal128(&mut parse_buffer, s.as_bytes())
    }

    fn from_f32(builder: &DecimalBuilder<Self>, value: f32) -> Result<Self> {
        scaled_f32_to_i128(value * builder.f32_factor)
    }

    fn from_scaled_f64(value: f64) -> Result<Self> {
        scaled_f64_to_i128(value)
    }
}

impl DecimalPrimitive for i256 {
    const DATA_TYPE_NAME: &'static str = "Decimal256";
    const ARRAY_BUILDER_VARIANT: fn(DecimalBuilder<Self>) -> ArrayBuilder =
        ArrayBuilder::Decimal256;
    const ARRAY_VARIANT: fn(DecimalArray<Self>) -> Array = Array::Decimal256;

    fn parse(parser: DecimalParser, s: &str) -> Result<Self> {
        let mut parse_buffer = [0; decimal::BUFFER_SIZE_I256];
        parser.parse_decimal256(&mut parse_buffer, s.as_bytes())
    }

    /// Scale in `f64`, `10^scale` exceeds the `f32` range for scales above 38
    fn from_f32(builder: &DecimalBuilder<Self>, value: f32) -> Result<Self> {
        decimal::scaled_f64_to_i256(f64::from(value) * builder.f64_factor)
    }

    fn from_scaled_f64(value: f64) -> Result<Self> {
        decimal::scaled_f64_to_i256(value)
    }
}

#[derive(Debug, Clone)]
pub struct DecimalBuilder<D: DecimalPrimitive> {
    pub name: String,
    pub precision: u8,
    pub scale: i8,
    pub f32_factor: f32,
    pub f64_factor: f64,
    pub parser: DecimalParser,
    pub array: PrimitiveArray<D>,
    metadata: HashMap<String, String>,
}

impl<D: DecimalPrimitive> DecimalBuilder<D> {
    pub fn new(
        name: String,
        precision: u8,
        scale: i8,
        is_nullable: bool,
        metadata: HashMap<String, String>,
    ) -> Self {
        Self {
            name,
            precision,
            scale,
            f32_factor: (10.0_f32).powi(scale as i32),
            f64_factor: (10.0_f64).powi(scale as i32),
            parser: DecimalParser::new(precision, scale, true),
            array: PrimitiveArray::new(is_nullable),
            metadata,
        }
    }

    pub fn take(&mut self) -> ArrayBuilder {
        D::ARRAY_BUILDER_VARIANT(Self {
            name: self.name.clone(),
            metadata: self.metadata.clone(),
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

    pub fn into_array_and_field_meta(self) -> Result<(Array, FieldMeta)> {
        let meta = FieldMeta {
            name: self.name,
            metadata: self.metadata,
            nullable: self.array.is_nullable(),
        };
        let array = D::ARRAY_VARIANT(DecimalArray {
            precision: self.precision,
            scale: self.scale,
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

    pub fn serialize_value<V: Serialize>(&mut self, value: V) -> Result<()> {
        value.serialize(&mut *self).ctx(self)
    }
}

impl<D: DecimalPrimitive> Context for DecimalBuilder<D> {
    fn annotate(&self, annotations: &mut BTreeMap<String, String>) {
        set_default(annotations, "field", &self.name);
        set_default(
            annotations,
            "data_type",
            format!(
                "{name}({precision}, {scale})",
                name = D::DATA_TYPE_NAME,
                precision = self.precision,
                scale = self.scale,
            ),
        );
    }
}

impl<'a, D: DecimalPrimitive> Serializer for &'a mut DecimalBuilder<D> {
    impl_serializer!(
        'a, DecimalBuilder;
        override serialize_none,
        override serialize_f32,
        override serialize_f64,
        override serialize_str,
    );

    fn serialize_none(self) -> Result<()> {
        self.array.push_scalar_none()
    }

    fn serialize_f32(self, v: f32) -> Result<()> {
        let value = D::from_f32(self, v)?;
        self.array.push_scalar_value(value)
    }

    fn serialize_f64(self, v: f64) -> Result<()> {
        self.array
            .push_scalar_value(D::from_scaled_f64(v * self.f64_factor)?)
    }

    fn serialize_str(self, v: &str) -> Result<()> {
        self.array.push_scalar_value(D::parse(self.parser, v)?)
    }
}

fn scaled_f32_to_i128(value: f32) -> Result<i128> {
    let exclusive_i128_bound = 2.0_f32.powi(127);

    if !value.is_finite() {
        fail!("cannot serialize non-finite float {value} as decimal");
    }
    if !(-exclusive_i128_bound..exclusive_i128_bound).contains(&value) {
        fail!("float value {value} is out of range for Decimal128");
    }

    #[expect(
        clippy::cast_possible_truncation,
        reason = "range and finite checks above rule out saturating float to int casts"
    )]
    {
        Ok(value as i128)
    }
}

fn scaled_f64_to_i128(value: f64) -> Result<i128> {
    let exclusive_i128_bound = 2.0_f64.powi(127);

    if !value.is_finite() {
        fail!("cannot serialize non-finite float {value} as decimal");
    }
    if !(-exclusive_i128_bound..exclusive_i128_bound).contains(&value) {
        fail!("float value {value} is out of range for Decimal128");
    }

    #[expect(
        clippy::cast_possible_truncation,
        reason = "range and finite checks above rule out saturating float to int casts"
    )]
    {
        Ok(value as i128)
    }
}
