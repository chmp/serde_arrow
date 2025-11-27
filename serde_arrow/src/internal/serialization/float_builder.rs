use std::collections::{BTreeMap, HashMap};

use half::f16;
use marrow::{
    array::{Array, PrimitiveArray},
    datatypes::FieldMeta,
};
use serde::{Serialize, Serializer};

use crate::internal::{
    error::{set_default, try_, Context, ContextSupport, Result},
    serialization::utils::impl_serializer,
    utils::array_ext::{ArrayExt, ScalarArrayExt},
};

use super::array_builder::ArrayBuilder;

pub trait FloatPrimitive: Sized + Copy + Default + 'static {
    const BUILDER: fn(FloatBuilder<Self>) -> ArrayBuilder;
    const ARRAY: fn(PrimitiveArray<Self>) -> Array;
    const NAME: &'static str;

    fn from_i8(value: i8) -> Self;
    fn from_i16(value: i16) -> Self;
    fn from_i32(value: i32) -> Self;
    fn from_i64(value: i64) -> Self;
    fn from_u8(value: u8) -> Self;
    fn from_u16(value: u16) -> Self;
    fn from_u32(value: u32) -> Self;
    fn from_u64(value: u64) -> Self;
    fn from_f32(value: f32) -> Self;
    fn from_f64(value: f64) -> Self;
}

impl FloatPrimitive for f16 {
    const BUILDER: fn(FloatBuilder<Self>) -> ArrayBuilder = ArrayBuilder::F16;
    const ARRAY: fn(PrimitiveArray<Self>) -> Array = Array::Float16;
    const NAME: &'static str = "Float16";

    fn from_i8(value: i8) -> Self {
        f16::from_f64(value as f64)
    }

    fn from_i16(value: i16) -> Self {
        f16::from_f64(value as f64)
    }

    fn from_i32(value: i32) -> Self {
        f16::from_f64(value as f64)
    }

    fn from_i64(value: i64) -> Self {
        f16::from_f64(value as f64)
    }

    fn from_u8(value: u8) -> Self {
        f16::from_f64(value as f64)
    }

    fn from_u16(value: u16) -> Self {
        f16::from_f64(value as f64)
    }

    fn from_u32(value: u32) -> Self {
        f16::from_f64(value as f64)
    }

    fn from_u64(value: u64) -> Self {
        f16::from_f64(value as f64)
    }

    fn from_f32(value: f32) -> Self {
        f16::from_f32(value)
    }

    fn from_f64(value: f64) -> Self {
        f16::from_f64(value)
    }
}

impl FloatPrimitive for f32 {
    const BUILDER: fn(FloatBuilder<Self>) -> ArrayBuilder = ArrayBuilder::F32;
    const ARRAY: fn(PrimitiveArray<Self>) -> Array = Array::Float32;
    const NAME: &'static str = "Float32";

    fn from_i8(value: i8) -> Self {
        value as f32
    }

    fn from_i16(value: i16) -> Self {
        value as f32
    }

    fn from_i32(value: i32) -> Self {
        value as f32
    }

    fn from_i64(value: i64) -> Self {
        value as f32
    }

    fn from_u8(value: u8) -> Self {
        value as f32
    }

    fn from_u16(value: u16) -> Self {
        value as f32
    }

    fn from_u32(value: u32) -> Self {
        value as f32
    }

    fn from_u64(value: u64) -> Self {
        value as f32
    }

    fn from_f32(value: f32) -> Self {
        value
    }

    fn from_f64(value: f64) -> Self {
        value as f32
    }
}

impl FloatPrimitive for f64 {
    const BUILDER: fn(FloatBuilder<Self>) -> ArrayBuilder = ArrayBuilder::F64;
    const ARRAY: fn(PrimitiveArray<Self>) -> Array = Array::Float64;
    const NAME: &'static str = "Float64";

    fn from_i8(value: i8) -> Self {
        value as f64
    }

    fn from_i16(value: i16) -> Self {
        value as f64
    }

    fn from_i32(value: i32) -> Self {
        value as f64
    }

    fn from_i64(value: i64) -> Self {
        value as f64
    }

    fn from_u8(value: u8) -> Self {
        value as f64
    }

    fn from_u16(value: u16) -> Self {
        value as f64
    }

    fn from_u32(value: u32) -> Self {
        value as f64
    }

    fn from_u64(value: u64) -> Self {
        value as f64
    }

    fn from_f32(value: f32) -> Self {
        value as f64
    }

    fn from_f64(value: f64) -> Self {
        value
    }
}

#[derive(Debug, Clone)]
pub struct FloatBuilder<I> {
    pub name: String,
    array: PrimitiveArray<I>,
    metadata: HashMap<String, String>,
}

impl<F: FloatPrimitive> FloatBuilder<F> {
    pub fn new(name: String, is_nullable: bool, metadata: HashMap<String, String>) -> Self {
        Self {
            name,
            array: PrimitiveArray::new(is_nullable),
            metadata,
        }
    }

    pub fn take_self(&mut self) -> Self {
        Self {
            name: self.name.clone(),
            metadata: self.metadata.clone(),
            array: self.array.take(),
        }
    }

    pub fn is_nullable(&self) -> bool {
        self.array.is_nullable()
    }

    pub fn reserve(&mut self, len: usize) {
        self.array.reserve(len);
    }

    pub fn take(&mut self) -> ArrayBuilder {
        F::BUILDER(self.take_self())
    }

    pub fn into_array_and_field_meta(self) -> Result<(Array, FieldMeta)> {
        let meta = FieldMeta {
            name: self.name,
            metadata: self.metadata,
            nullable: self.array.is_nullable(),
        };
        Ok((F::ARRAY(self.array), meta))
    }

    pub fn serialize_default_value(&mut self) -> Result<()> {
        try_(|| self.array.push_scalar_default()).ctx(self)
    }

    pub fn serialize_value<V: Serialize>(&mut self, value: V) -> Result<()> {
        value.serialize(&mut *self).ctx(self)
    }
}

impl<F: FloatPrimitive> Context for FloatBuilder<F> {
    fn annotate(&self, annotations: &mut BTreeMap<String, String>) {
        set_default(annotations, "field", &self.name);
        set_default(annotations, "data_type", F::NAME);
    }
}

impl<'a, F: FloatPrimitive> Serializer for &'a mut FloatBuilder<F> {
    impl_serializer!(
        'a, FloatBuilder;
        override serialize_none,
        override serialize_i8,
        override serialize_i16,
        override serialize_i32,
        override serialize_i64,
        override serialize_u8,
        override serialize_u16,
        override serialize_u32,
        override serialize_u64,
        override serialize_f32,
        override serialize_f64,
    );

    fn serialize_none(self) -> Result<()> {
        self.array.push_scalar_none()
    }

    fn serialize_i8(self, v: i8) -> Result<()> {
        self.array.push_scalar_value(F::from_i8(v))
    }

    fn serialize_i16(self, v: i16) -> Result<()> {
        self.array.push_scalar_value(F::from_i16(v))
    }

    fn serialize_i32(self, v: i32) -> Result<()> {
        self.array.push_scalar_value(F::from_i32(v))
    }

    fn serialize_i64(self, v: i64) -> Result<()> {
        self.array.push_scalar_value(F::from_i64(v))
    }

    fn serialize_u8(self, v: u8) -> Result<()> {
        self.array.push_scalar_value(F::from_u8(v))
    }

    fn serialize_u16(self, v: u16) -> Result<()> {
        self.array.push_scalar_value(F::from_u16(v))
    }

    fn serialize_u32(self, v: u32) -> Result<()> {
        self.array.push_scalar_value(F::from_u32(v))
    }

    fn serialize_u64(self, v: u64) -> Result<()> {
        self.array.push_scalar_value(F::from_u64(v))
    }

    fn serialize_f32(self, v: f32) -> Result<()> {
        self.array.push_scalar_value(F::from_f32(v))
    }

    fn serialize_f64(self, v: f64) -> Result<()> {
        self.array.push_scalar_value(F::from_f64(v))
    }
}
