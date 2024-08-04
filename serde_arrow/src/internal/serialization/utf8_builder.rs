use crate::internal::{
    arrow::{Array, BytesArray},
    error::{fail, Result},
    utils::Offset,
};

use super::{
    array_ext::{new_bytes_array, ArrayExt, ScalarArrayExt},
    simple_serializer::SimpleSerializer,
};

#[derive(Debug, Clone)]
pub struct Utf8Builder<O>(BytesArray<O>);

impl<O: Offset> Utf8Builder<O> {
    pub fn new(is_nullable: bool) -> Self {
        Self(new_bytes_array(is_nullable))
    }

    pub fn take(&mut self) -> Self {
        Self(self.0.take())
    }

    pub fn is_nullable(&self) -> bool {
        self.0.validity.is_some()
    }
}

impl Utf8Builder<i32> {
    pub fn into_array(self) -> Result<Array> {
        Ok(Array::Utf8(self.0))
    }
}

impl Utf8Builder<i64> {
    pub fn into_array(self) -> Result<Array> {
        Ok(Array::LargeUtf8(self.0))
    }
}

impl<O: Offset> SimpleSerializer for Utf8Builder<O> {
    fn name(&self) -> &str {
        "Utf8Builder"
    }

    fn serialize_default(&mut self) -> Result<()> {
        self.0.push_scalar_default()
    }

    fn serialize_none(&mut self) -> Result<()> {
        self.0.push_scalar_none()
    }

    fn serialize_str(&mut self, v: &str) -> Result<()> {
        self.0.push_scalar_value(v.as_bytes())
    }

    fn serialize_unit_variant(
        &mut self,
        _: &'static str,
        _: u32,
        variant: &'static str,
    ) -> Result<()> {
        self.0.push_scalar_value(variant.as_bytes())
    }

    fn serialize_tuple_variant_start<'this>(
        &'this mut self,
        _: &'static str,
        _: u32,
        _: &'static str,
        _: usize,
    ) -> Result<&'this mut super::ArrayBuilder> {
        fail!("Cannot serialize enum with data as string");
    }

    fn serialize_struct_variant_start<'this>(
        &'this mut self,
        _: &'static str,
        _: u32,
        _: &'static str,
        _: usize,
    ) -> Result<&'this mut super::ArrayBuilder> {
        fail!("Cannot serialize enum with data as string");
    }

    fn serialize_newtype_variant<V: serde::Serialize + ?Sized>(
        &mut self,
        _: &'static str,
        _: u32,
        _: &'static str,
        _: &V,
    ) -> Result<()> {
        fail!("Cannot serialize enum with data as string");
    }
}
