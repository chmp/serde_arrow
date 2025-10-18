use std::collections::BTreeMap;

use half::f16;
use marrow::array::{Array, PrimitiveArray};

use crate::internal::{
    error::{set_default, try_, Context, ContextSupport, Result},
    serialization::utils::impl_serializer,
    utils::{
        array_ext::{ArrayExt, ScalarArrayExt},
        Mut,
    },
};

use super::{array_builder::ArrayBuilder, simple_serializer::SimpleSerializer};

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
    path: String,
    array: PrimitiveArray<I>,
}

impl<F: FloatPrimitive> FloatBuilder<F> {
    pub fn new(path: String, is_nullable: bool) -> Self {
        Self {
            path,
            array: PrimitiveArray::new(is_nullable),
        }
    }

    pub fn take_self(&mut self) -> Self {
        Self {
            path: self.path.clone(),
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

    pub fn into_array(self) -> Result<Array> {
        Ok(F::ARRAY(self.array))
    }

    pub fn serialize_default_value(&mut self) -> Result<()> {
        try_(|| self.array.push_scalar_default()).ctx(self)
    }
}

impl<F: FloatPrimitive> Context for FloatBuilder<F> {
    fn annotate(&self, annotations: &mut BTreeMap<String, String>) {
        set_default(annotations, "field", &self.path);
        set_default(annotations, "data_type", F::NAME);
    }
}

impl<F: FloatPrimitive> SimpleSerializer for FloatBuilder<F> {
    fn serialize_default(&mut self) -> Result<()> {
        self.serialize_default_value()
    }

    fn serialize_none(&mut self) -> Result<()> {
        try_(|| self.array.push_scalar_none()).ctx(self)
    }

    fn serialize_some<V: serde::Serialize + ?Sized>(&mut self, value: &V) -> Result<()> {
        try_(|| value.serialize(Mut(&mut *self))).ctx(self)
    }

    fn serialize_i8(&mut self, v: i8) -> Result<()> {
        try_(|| self.array.push_scalar_value(F::from_i8(v))).ctx(self)
    }

    fn serialize_i16(&mut self, v: i16) -> Result<()> {
        try_(|| self.array.push_scalar_value(F::from_i16(v))).ctx(self)
    }

    fn serialize_i32(&mut self, v: i32) -> Result<()> {
        try_(|| self.array.push_scalar_value(F::from_i32(v))).ctx(self)
    }

    fn serialize_i64(&mut self, v: i64) -> Result<()> {
        try_(|| self.array.push_scalar_value(F::from_i64(v))).ctx(self)
    }

    fn serialize_u8(&mut self, v: u8) -> Result<()> {
        try_(|| self.array.push_scalar_value(F::from_u8(v))).ctx(self)
    }

    fn serialize_u16(&mut self, v: u16) -> Result<()> {
        try_(|| self.array.push_scalar_value(F::from_u16(v))).ctx(self)
    }

    fn serialize_u32(&mut self, v: u32) -> Result<()> {
        try_(|| self.array.push_scalar_value(F::from_u32(v))).ctx(self)
    }

    fn serialize_u64(&mut self, v: u64) -> Result<()> {
        try_(|| self.array.push_scalar_value(F::from_u64(v))).ctx(self)
    }

    fn serialize_f32(&mut self, v: f32) -> Result<()> {
        try_(|| self.array.push_scalar_value(F::from_f32(v))).ctx(self)
    }

    fn serialize_f64(&mut self, v: f64) -> Result<()> {
        try_(|| self.array.push_scalar_value(F::from_f64(v))).ctx(self)
    }
}

impl<'a, F: FloatPrimitive> serde::Serializer for &'a mut FloatBuilder<F> {
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
        try_(|| self.array.push_scalar_none()).ctx(self)
    }

    fn serialize_i8(self, v: i8) -> Result<()> {
        try_(|| self.array.push_scalar_value(F::from_i8(v))).ctx(self)
    }

    fn serialize_i16(self, v: i16) -> Result<()> {
        try_(|| self.array.push_scalar_value(F::from_i16(v))).ctx(self)
    }

    fn serialize_i32(self, v: i32) -> Result<()> {
        try_(|| self.array.push_scalar_value(F::from_i32(v))).ctx(self)
    }

    fn serialize_i64(self, v: i64) -> Result<()> {
        try_(|| self.array.push_scalar_value(F::from_i64(v))).ctx(self)
    }

    fn serialize_u8(self, v: u8) -> Result<()> {
        try_(|| self.array.push_scalar_value(F::from_u8(v))).ctx(self)
    }

    fn serialize_u16(self, v: u16) -> Result<()> {
        try_(|| self.array.push_scalar_value(F::from_u16(v))).ctx(self)
    }

    fn serialize_u32(self, v: u32) -> Result<()> {
        try_(|| self.array.push_scalar_value(F::from_u32(v))).ctx(self)
    }

    fn serialize_u64(self, v: u64) -> Result<()> {
        try_(|| self.array.push_scalar_value(F::from_u64(v))).ctx(self)
    }

    fn serialize_f32(self, v: f32) -> Result<()> {
        try_(|| self.array.push_scalar_value(F::from_f32(v))).ctx(self)
    }

    fn serialize_f64(self, v: f64) -> Result<()> {
        try_(|| self.array.push_scalar_value(F::from_f64(v))).ctx(self)
    }
}
