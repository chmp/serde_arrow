use std::collections::BTreeMap;

use marrow::array::{Array, BytesArray, BytesViewArray};

use crate::internal::{
    error::{fail, set_default, try_, Context, ContextSupport, Result},
    utils::array_ext::{ArrayExt, ScalarArrayExt},
};

use super::{array_builder::ArrayBuilder, simple_serializer::SimpleSerializer};

pub trait Utf8BuilderArray:
    ArrayExt + for<'s> ScalarArrayExt<'s, Value = &'s [u8]> + Sized
{
    const DATA_TYPE_NAME: &'static str;
    const ARRAY_BUILDER_VARIANT: fn(Utf8Builder<Self>) -> ArrayBuilder;
    const ARRAY_VARIANT: fn(Self) -> Array;
}

impl Utf8BuilderArray for BytesArray<i32> {
    const DATA_TYPE_NAME: &'static str = "Utf8";
    const ARRAY_BUILDER_VARIANT: fn(Utf8Builder<Self>) -> ArrayBuilder = ArrayBuilder::Utf8;
    const ARRAY_VARIANT: fn(Self) -> Array = Array::Utf8;
}

impl Utf8BuilderArray for BytesArray<i64> {
    const DATA_TYPE_NAME: &'static str = "LargeUtf8";
    const ARRAY_BUILDER_VARIANT: fn(Utf8Builder<Self>) -> ArrayBuilder = ArrayBuilder::LargeUtf8;
    const ARRAY_VARIANT: fn(Self) -> Array = Array::LargeUtf8;
}

impl Utf8BuilderArray for BytesViewArray {
    const DATA_TYPE_NAME: &'static str = "Utf8View";
    const ARRAY_BUILDER_VARIANT: fn(Utf8Builder<Self>) -> ArrayBuilder = ArrayBuilder::Utf8View;
    const ARRAY_VARIANT: fn(Self) -> Array = Array::Utf8View;
}

#[derive(Debug, Clone)]
pub struct Utf8Builder<A> {
    path: String,
    array: A,
}

impl<A: Utf8BuilderArray> Utf8Builder<A> {
    pub fn new(path: String, is_nullable: bool) -> Self {
        Self {
            path,
            array: A::new(is_nullable),
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

    pub fn take(&mut self) -> ArrayBuilder {
        A::ARRAY_BUILDER_VARIANT(self.take_self())
    }

    pub fn into_array(self) -> Result<Array> {
        Ok(A::ARRAY_VARIANT(self.array))
    }
}

impl<A: Utf8BuilderArray> Context for Utf8Builder<A> {
    fn annotate(&self, annotations: &mut BTreeMap<String, String>) {
        set_default(annotations, "field", &self.path);
        set_default(annotations, "data_type", A::DATA_TYPE_NAME);
    }
}

impl<A: Utf8BuilderArray> SimpleSerializer for Utf8Builder<A> {
    fn serialize_default(&mut self) -> Result<()> {
        try_(|| self.array.push_scalar_default()).ctx(self)
    }

    fn serialize_none(&mut self) -> Result<()> {
        try_(|| self.array.push_scalar_none()).ctx(self)
    }

    fn serialize_str(&mut self, v: &str) -> Result<()> {
        try_(|| self.array.push_scalar_value(v.as_bytes())).ctx(self)
    }

    fn serialize_i8(&mut self, v: i8) -> Result<()> {
        try_(|| self.array.push_scalar_value(v.to_string().as_bytes())).ctx(self)
    }

    fn serialize_i16(&mut self, v: i16) -> Result<()> {
        try_(|| self.array.push_scalar_value(v.to_string().as_bytes())).ctx(self)
    }

    fn serialize_i32(&mut self, v: i32) -> Result<()> {
        try_(|| self.array.push_scalar_value(v.to_string().as_bytes())).ctx(self)
    }

    fn serialize_i64(&mut self, v: i64) -> Result<()> {
        try_(|| self.array.push_scalar_value(v.to_string().as_bytes())).ctx(self)
    }

    fn serialize_u8(&mut self, v: u8) -> Result<()> {
        try_(|| self.array.push_scalar_value(v.to_string().as_bytes())).ctx(self)
    }

    fn serialize_u16(&mut self, v: u16) -> Result<()> {
        try_(|| self.array.push_scalar_value(v.to_string().as_bytes())).ctx(self)
    }

    fn serialize_u32(&mut self, v: u32) -> Result<()> {
        try_(|| self.array.push_scalar_value(v.to_string().as_bytes())).ctx(self)
    }

    fn serialize_u64(&mut self, v: u64) -> Result<()> {
        try_(|| self.array.push_scalar_value(v.to_string().as_bytes())).ctx(self)
    }

    fn serialize_f32(&mut self, v: f32) -> Result<()> {
        try_(|| self.array.push_scalar_value(v.to_string().as_bytes())).ctx(self)
    }

    fn serialize_f64(&mut self, v: f64) -> Result<()> {
        try_(|| self.array.push_scalar_value(v.to_string().as_bytes())).ctx(self)
    }

    fn serialize_char(&mut self, v: char) -> Result<()> {
        try_(|| self.array.push_scalar_value(v.to_string().as_bytes())).ctx(self)
    }

    fn serialize_bool(&mut self, v: bool) -> Result<()> {
        try_(|| self.array.push_scalar_value(v.to_string().as_bytes())).ctx(self)
    }

    fn serialize_unit_variant(
        &mut self,
        _: &'static str,
        _: u32,
        variant: &'static str,
    ) -> Result<()> {
        try_(|| self.array.push_scalar_value(variant.as_bytes())).ctx(self)
    }

    fn serialize_tuple_variant_start<'this>(
        &'this mut self,
        _: &'static str,
        _: u32,
        _: &'static str,
        _: usize,
    ) -> Result<&'this mut super::ArrayBuilder> {
        fail!(in self, "Cannot serialize enum with data as string");
    }

    fn serialize_struct_variant_start<'this>(
        &'this mut self,
        _: &'static str,
        _: u32,
        _: &'static str,
        _: usize,
    ) -> Result<&'this mut super::ArrayBuilder> {
        fail!(in self, "Cannot serialize enum with data as string");
    }

    fn serialize_newtype_variant<V: serde::Serialize + ?Sized>(
        &mut self,
        _: &'static str,
        _: u32,
        _: &'static str,
        _: &V,
    ) -> Result<()> {
        fail!(in self, "Cannot serialize enum with data as string");
    }
}
