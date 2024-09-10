use std::collections::BTreeMap;

use half::f16;

use crate::internal::{
    arrow::{Array, PrimitiveArray},
    error::{set_default, try_, Context, ContextSupport, Result},
    utils::{
        array_ext::{new_primitive_array, ArrayExt, ScalarArrayExt},
        Mut,
    },
};

use super::{array_builder::ArrayBuilder, simple_serializer::SimpleSerializer};

#[derive(Debug, Clone)]
pub struct FloatBuilder<I> {
    path: String,
    array: PrimitiveArray<I>,
}

impl<F: Default + 'static> FloatBuilder<F> {
    pub fn new(path: String, is_nullable: bool) -> Self {
        Self {
            path,
            array: new_primitive_array(is_nullable),
        }
    }

    pub fn take_self(&mut self) -> Self {
        Self {
            path: self.path.clone(),
            array: self.array.take(),
        }
    }

    pub fn is_nullable(&self) -> bool {
        self.array.validity.is_some()
    }
}

macro_rules! impl_into_array {
    ($ty:ty, $builder_var:ident, $array_var:ident) => {
        impl FloatBuilder<$ty> {
            pub fn take(&mut self) -> ArrayBuilder {
                ArrayBuilder::$builder_var(self.take_self())
            }

            pub fn into_array(self) -> Result<Array> {
                Ok(Array::$array_var(self.array))
            }
        }
    };
}

impl_into_array!(f16, F16, Float16);
impl_into_array!(f32, F32, Float32);
impl_into_array!(f64, F64, Float64);

impl Context for FloatBuilder<f16> {
    fn annotate(&self, annotations: &mut BTreeMap<String, String>) {
        set_default(annotations, "field", &self.path);
        set_default(annotations, "data_type", "Float16");
    }
}

impl Context for FloatBuilder<f32> {
    fn annotate(&self, annotations: &mut BTreeMap<String, String>) {
        set_default(annotations, "field", &self.path);
        set_default(annotations, "data_type", "Float32");
    }
}

impl Context for FloatBuilder<f64> {
    fn annotate(&self, annotations: &mut BTreeMap<String, String>) {
        set_default(annotations, "field", &self.path);
        set_default(annotations, "data_type", "Float64");
    }
}

impl SimpleSerializer for FloatBuilder<f32> {
    fn serialize_default(&mut self) -> Result<()> {
        try_(|| self.array.push_scalar_default()).ctx(self)
    }

    fn serialize_none(&mut self) -> Result<()> {
        try_(|| self.array.push_scalar_none()).ctx(self)
    }

    fn serialize_some<V: serde::Serialize + ?Sized>(&mut self, value: &V) -> Result<()> {
        try_(|| value.serialize(Mut(&mut *self))).ctx(self)
    }

    fn serialize_i8(&mut self, v: i8) -> Result<()> {
        try_(|| self.array.push_scalar_value(v as f32)).ctx(self)
    }

    fn serialize_i16(&mut self, v: i16) -> Result<()> {
        try_(|| self.array.push_scalar_value(v as f32)).ctx(self)
    }

    fn serialize_i32(&mut self, v: i32) -> Result<()> {
        try_(|| self.array.push_scalar_value(v as f32)).ctx(self)
    }

    fn serialize_i64(&mut self, v: i64) -> Result<()> {
        try_(|| self.array.push_scalar_value(v as f32)).ctx(self)
    }

    fn serialize_u8(&mut self, v: u8) -> Result<()> {
        try_(|| self.array.push_scalar_value(v as f32)).ctx(self)
    }

    fn serialize_u16(&mut self, v: u16) -> Result<()> {
        try_(|| self.array.push_scalar_value(v as f32)).ctx(self)
    }

    fn serialize_u32(&mut self, v: u32) -> Result<()> {
        try_(|| self.array.push_scalar_value(v as f32)).ctx(self)
    }

    fn serialize_u64(&mut self, v: u64) -> Result<()> {
        try_(|| self.array.push_scalar_value(v as f32)).ctx(self)
    }

    fn serialize_f32(&mut self, v: f32) -> Result<()> {
        try_(|| self.array.push_scalar_value(v)).ctx(self)
    }

    fn serialize_f64(&mut self, v: f64) -> Result<()> {
        try_(|| self.array.push_scalar_value(v as f32)).ctx(self)
    }
}

impl SimpleSerializer for FloatBuilder<f64> {
    fn serialize_default(&mut self) -> Result<()> {
        try_(|| self.array.push_scalar_default()).ctx(self)
    }

    fn serialize_none(&mut self) -> Result<()> {
        try_(|| self.array.push_scalar_none()).ctx(self)
    }

    fn serialize_i8(&mut self, v: i8) -> Result<()> {
        try_(|| self.array.push_scalar_value(v as f64)).ctx(self)
    }

    fn serialize_i16(&mut self, v: i16) -> Result<()> {
        try_(|| self.array.push_scalar_value(v as f64)).ctx(self)
    }

    fn serialize_i32(&mut self, v: i32) -> Result<()> {
        try_(|| self.array.push_scalar_value(v as f64)).ctx(self)
    }

    fn serialize_i64(&mut self, v: i64) -> Result<()> {
        try_(|| self.array.push_scalar_value(v as f64)).ctx(self)
    }

    fn serialize_u8(&mut self, v: u8) -> Result<()> {
        try_(|| self.array.push_scalar_value(v as f64)).ctx(self)
    }

    fn serialize_u16(&mut self, v: u16) -> Result<()> {
        try_(|| self.array.push_scalar_value(v as f64)).ctx(self)
    }

    fn serialize_u32(&mut self, v: u32) -> Result<()> {
        try_(|| self.array.push_scalar_value(v as f64)).ctx(self)
    }

    fn serialize_u64(&mut self, v: u64) -> Result<()> {
        try_(|| self.array.push_scalar_value(v as f64)).ctx(self)
    }

    fn serialize_f32(&mut self, v: f32) -> Result<()> {
        try_(|| self.array.push_scalar_value(v as f64)).ctx(self)
    }

    fn serialize_f64(&mut self, v: f64) -> Result<()> {
        try_(|| self.array.push_scalar_value(v)).ctx(self)
    }
}

impl SimpleSerializer for FloatBuilder<f16> {
    fn serialize_default(&mut self) -> Result<()> {
        try_(|| self.array.push_scalar_default()).ctx(self)
    }

    fn serialize_none(&mut self) -> Result<()> {
        try_(|| self.array.push_scalar_none()).ctx(self)
    }

    fn serialize_f32(&mut self, v: f32) -> Result<()> {
        try_(|| self.array.push_scalar_value(f16::from_f32(v))).ctx(self)
    }

    fn serialize_f64(&mut self, v: f64) -> Result<()> {
        try_(|| self.array.push_scalar_value(f16::from_f64(v))).ctx(self)
    }
}
