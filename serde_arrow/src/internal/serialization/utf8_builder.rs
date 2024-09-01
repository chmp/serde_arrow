use std::collections::BTreeMap;

use crate::internal::{
    arrow::{Array, BytesArray},
    error::{fail, Context, ContextSupport, Result},
    utils::{
        array_ext::{new_bytes_array, ArrayExt, ScalarArrayExt},
        btree_map, Offset,
    },
};

use super::simple_serializer::SimpleSerializer;

#[derive(Debug, Clone)]
pub struct Utf8Builder<O> {
    path: String,
    array: BytesArray<O>,
}

impl<O: Offset> Utf8Builder<O> {
    pub fn new(path: String, is_nullable: bool) -> Self {
        Self {
            path,
            array: new_bytes_array(is_nullable),
        }
    }

    pub fn take(&mut self) -> Self {
        Self {
            path: self.path.clone(),
            array: self.array.take(),
        }
    }

    pub fn is_nullable(&self) -> bool {
        self.array.validity.is_some()
    }
}

impl Utf8Builder<i32> {
    pub fn into_array(self) -> Result<Array> {
        Ok(Array::Utf8(self.array))
    }
}

impl Utf8Builder<i64> {
    pub fn into_array(self) -> Result<Array> {
        Ok(Array::LargeUtf8(self.array))
    }
}

impl<O> Context for Utf8Builder<O> {
    fn annotations(&self) -> BTreeMap<String, String> {
        btree_map!("field" => self.path.clone())
    }
}

impl<O: Offset> SimpleSerializer for Utf8Builder<O> {
    fn serialize_default(&mut self) -> Result<()> {
        self.array.push_scalar_default().ctx(self)
    }

    fn serialize_none(&mut self) -> Result<()> {
        self.array.push_scalar_none().ctx(self)
    }

    fn serialize_str(&mut self, v: &str) -> Result<()> {
        self.array.push_scalar_value(v.as_bytes()).ctx(self)
    }

    fn serialize_unit_variant(
        &mut self,
        _: &'static str,
        _: u32,
        variant: &'static str,
    ) -> Result<()> {
        self.array.push_scalar_value(variant.as_bytes()).ctx(self)
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
