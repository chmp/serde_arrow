use std::collections::{BTreeMap, HashMap};

use marrow::{
    array::{Array, BytesArray, BytesViewArray},
    datatypes::FieldMeta,
};
use serde::{Serialize, Serializer};

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
    pub name: String,
    array: A,
    metadata: HashMap<String, String>,
}

impl<A: Utf8BuilderArray> Utf8Builder<A> {
    pub fn new(path: String, is_nullable: bool, metadata: HashMap<String, String>) -> Self {
        Self {
            name: path,
            array: A::new(is_nullable),
            metadata,
        }
    }

    pub fn is_nullable(&self) -> bool {
        self.array.is_nullable()
    }

    pub fn take(&mut self) -> ArrayBuilder {
        A::ARRAY_BUILDER_VARIANT(Self {
            name: self.name.clone(),
            metadata: self.metadata.clone(),
            array: self.array.take(),
        })
    }

    pub fn into_array_and_field_meta(self) -> Result<(Array, FieldMeta)> {
        let meta = FieldMeta {
            name: self.name,
            metadata: self.metadata,
            nullable: self.array.is_nullable(),
        };
        let array = A::ARRAY_VARIANT(self.array);
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

impl<A: Utf8BuilderArray> Context for Utf8Builder<A> {
    fn annotate(&self, annotations: &mut BTreeMap<String, String>) {
        set_default(annotations, "field", &self.name);
        set_default(annotations, "data_type", A::DATA_TYPE_NAME);
    }
}

impl<'a, A: Utf8BuilderArray> Serializer for &'a mut Utf8Builder<A> {
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
        self.array.push_scalar_none()
    }

    fn serialize_str(self, v: &str) -> Result<()> {
        self.array.push_scalar_value(v.as_bytes())
    }

    fn serialize_i8(self, v: i8) -> Result<()> {
        self.array.push_scalar_value(v.to_string().as_bytes())
    }

    fn serialize_i16(self, v: i16) -> Result<()> {
        self.array.push_scalar_value(v.to_string().as_bytes())
    }

    fn serialize_i32(self, v: i32) -> Result<()> {
        self.array.push_scalar_value(v.to_string().as_bytes())
    }

    fn serialize_i64(self, v: i64) -> Result<()> {
        self.array.push_scalar_value(v.to_string().as_bytes())
    }

    fn serialize_u8(self, v: u8) -> Result<()> {
        self.array.push_scalar_value(v.to_string().as_bytes())
    }

    fn serialize_u16(self, v: u16) -> Result<()> {
        self.array.push_scalar_value(v.to_string().as_bytes())
    }

    fn serialize_u32(self, v: u32) -> Result<()> {
        self.array.push_scalar_value(v.to_string().as_bytes())
    }

    fn serialize_u64(self, v: u64) -> Result<()> {
        self.array.push_scalar_value(v.to_string().as_bytes())
    }

    fn serialize_f32(self, v: f32) -> Result<()> {
        self.array.push_scalar_value(v.to_string().as_bytes())
    }

    fn serialize_f64(self, v: f64) -> Result<()> {
        self.array.push_scalar_value(v.to_string().as_bytes())
    }

    fn serialize_char(self, v: char) -> Result<()> {
        self.array.push_scalar_value(v.to_string().as_bytes())
    }

    fn serialize_bool(self, v: bool) -> Result<()> {
        self.array.push_scalar_value(v.to_string().as_bytes())
    }

    fn serialize_unit_variant(self, _: &'static str, _: u32, variant: &'static str) -> Result<()> {
        self.array.push_scalar_value(variant.as_bytes())
    }

    fn serialize_tuple_variant(
        self,
        _: &'static str,
        _: u32,
        _: &'static str,
        _: usize,
    ) -> Result<Self::SerializeTupleVariant> {
        fail!("Cannot serialize enum with data as string");
    }

    fn serialize_struct_variant(
        self,
        _: &'static str,
        _: u32,
        _: &'static str,
        _: usize,
    ) -> Result<Self::SerializeStructVariant> {
        fail!("Cannot serialize enum with data as string");
    }

    fn serialize_newtype_variant<V: serde::Serialize + ?Sized>(
        self,
        _: &'static str,
        _: u32,
        _: &'static str,
        _: &V,
    ) -> Result<()> {
        fail!("Cannot serialize enum with data as string");
    }
}
