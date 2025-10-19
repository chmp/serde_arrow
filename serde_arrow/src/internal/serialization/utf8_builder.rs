use std::collections::BTreeMap;

use marrow::array::{Array, BytesArray, BytesViewArray};

use crate::internal::{
    error::{fail, set_default, try_, Context, ContextSupport, Result},
    serialization::utils::impl_serializer,
    utils::array_ext::{ArrayExt, ScalarArrayExt},
};

use super::array_builder::ArrayBuilder;

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

    pub fn reserve(&mut self, additional: usize) {
        self.array.reserve(additional);
    }

    pub fn serialize_default_value(&mut self) -> Result<()> {
        try_(|| self.array.push_scalar_default()).ctx(self)
    }
}

impl<A: Utf8BuilderArray> Context for Utf8Builder<A> {
    fn annotate(&self, annotations: &mut BTreeMap<String, String>) {
        set_default(annotations, "field", &self.path);
        set_default(annotations, "data_type", A::DATA_TYPE_NAME);
    }
}

impl<'a, A: Utf8BuilderArray> serde::Serializer for &'a mut Utf8Builder<A> {
    impl_serializer!(
        'a, Utf8Builder;
        override serialize_none,
        override serialize_str,
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
        override serialize_char,
        override serialize_bool,
        override serialize_unit_variant,
        override serialize_tuple_variant,
        override serialize_struct_variant,
        override serialize_newtype_variant,
    );

    fn serialize_none(self) -> Result<()> {
        try_(|| self.array.push_scalar_none()).ctx(self)
    }

    fn serialize_str(self, v: &str) -> Result<()> {
        try_(|| self.array.push_scalar_value(v.as_bytes())).ctx(self)
    }

    fn serialize_i8(self, v: i8) -> Result<()> {
        try_(|| self.array.push_scalar_value(v.to_string().as_bytes())).ctx(self)
    }

    fn serialize_i16(self, v: i16) -> Result<()> {
        try_(|| self.array.push_scalar_value(v.to_string().as_bytes())).ctx(self)
    }

    fn serialize_i32(self, v: i32) -> Result<()> {
        try_(|| self.array.push_scalar_value(v.to_string().as_bytes())).ctx(self)
    }

    fn serialize_i64(self, v: i64) -> Result<()> {
        try_(|| self.array.push_scalar_value(v.to_string().as_bytes())).ctx(self)
    }

    fn serialize_u8(self, v: u8) -> Result<()> {
        try_(|| self.array.push_scalar_value(v.to_string().as_bytes())).ctx(self)
    }

    fn serialize_u16(self, v: u16) -> Result<()> {
        try_(|| self.array.push_scalar_value(v.to_string().as_bytes())).ctx(self)
    }

    fn serialize_u32(self, v: u32) -> Result<()> {
        try_(|| self.array.push_scalar_value(v.to_string().as_bytes())).ctx(self)
    }

    fn serialize_u64(self, v: u64) -> Result<()> {
        try_(|| self.array.push_scalar_value(v.to_string().as_bytes())).ctx(self)
    }

    fn serialize_f32(self, v: f32) -> Result<()> {
        try_(|| self.array.push_scalar_value(v.to_string().as_bytes())).ctx(self)
    }

    fn serialize_f64(self, v: f64) -> Result<()> {
        try_(|| self.array.push_scalar_value(v.to_string().as_bytes())).ctx(self)
    }

    fn serialize_char(self, v: char) -> Result<()> {
        try_(|| self.array.push_scalar_value(v.to_string().as_bytes())).ctx(self)
    }

    fn serialize_bool(self, v: bool) -> Result<()> {
        try_(|| self.array.push_scalar_value(v.to_string().as_bytes())).ctx(self)
    }

    fn serialize_unit_variant(self, _: &'static str, _: u32, variant: &'static str) -> Result<()> {
        try_(|| self.array.push_scalar_value(variant.as_bytes())).ctx(self)
    }

    fn serialize_tuple_variant(
        self,
        _: &'static str,
        _: u32,
        _: &'static str,
        _: usize,
    ) -> Result<Self::SerializeTupleVariant> {
        fail!(in self, "Cannot serialize enum with data as string");
    }

    fn serialize_struct_variant(
        self,
        _: &'static str,
        _: u32,
        _: &'static str,
        _: usize,
    ) -> Result<Self::SerializeStructVariant> {
        fail!(in self, "Cannot serialize enum with data as string");
    }

    fn serialize_newtype_variant<V: serde::Serialize + ?Sized>(
        self,
        _: &'static str,
        _: u32,
        _: &'static str,
        _: &V,
    ) -> Result<()> {
        fail!(in self, "Cannot serialize enum with data as string");
    }
}
