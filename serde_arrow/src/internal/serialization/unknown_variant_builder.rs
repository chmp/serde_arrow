use std::collections::BTreeMap;

use serde::Serialize;

use crate::internal::{
    arrow::{Array, NullArray},
    error::{fail, Context, Result},
    utils::btree_map,
};

use super::{array_builder::ArrayBuilder, simple_serializer::SimpleSerializer};

#[derive(Debug, Clone)]
pub struct UnknownVariantBuilder {
    path: String,
}

impl UnknownVariantBuilder {
    pub fn new(path: String) -> Self {
        UnknownVariantBuilder { path }
    }

    pub fn take(&mut self) -> ArrayBuilder {
        ArrayBuilder::UnknownVariant(UnknownVariantBuilder {
            path: self.path.clone(),
        })
    }

    pub fn is_nullable(&self) -> bool {
        false
    }

    pub fn into_array(self) -> Result<Array> {
        Ok(Array::Null(NullArray { len: 0 }))
    }
}

impl Context for UnknownVariantBuilder {
    fn annotations(&self) -> BTreeMap<String, String> {
        btree_map!("field" => self.path.clone(), "data_type" => "<unknown variant>")
    }
}

impl SimpleSerializer for UnknownVariantBuilder {
    fn serialize_default(&mut self) -> Result<()> {
        fail!(in self, "Unknown variant does not support serialize_default")
    }

    fn serialize_unit(&mut self) -> Result<()> {
        fail!(in self, "Unknown variant does not support serialize_unit")
    }

    fn serialize_none(&mut self) -> Result<()> {
        fail!(in self, "Unknown variant does not support serialize_none")
    }

    fn serialize_bool(&mut self, _: bool) -> Result<()> {
        fail!(in self, "Unknown variant does not support serialize_bool")
    }

    fn serialize_char(&mut self, _: char) -> Result<()> {
        fail!(in self, "Unknown variant does not support serialize_char")
    }

    fn serialize_u8(&mut self, _: u8) -> Result<()> {
        fail!(in self, "Unknown variant does not support serialize_u8")
    }

    fn serialize_u16(&mut self, _: u16) -> Result<()> {
        fail!(in self, "Unknown variant does not support serialize_u16")
    }

    fn serialize_u32(&mut self, _: u32) -> Result<()> {
        fail!(in self, "Unknown variant does not support serialize_u32")
    }

    fn serialize_u64(&mut self, _: u64) -> Result<()> {
        fail!(in self, "Unknown variant does not support serialize_u64")
    }

    fn serialize_i8(&mut self, _: i8) -> Result<()> {
        fail!(in self, "Unknown variant does not support serialize_i8")
    }

    fn serialize_i16(&mut self, _: i16) -> Result<()> {
        fail!(in self, "Unknown variant does not support serialize_i16")
    }

    fn serialize_i32(&mut self, _: i32) -> Result<()> {
        fail!(in self, "Unknown variant does not support serialize_i32")
    }

    fn serialize_i64(&mut self, _: i64) -> Result<()> {
        fail!(in self, "Unknown variant does not support serialize_i64")
    }

    fn serialize_f32(&mut self, _: f32) -> Result<()> {
        fail!(in self, "Unknown variant does not support serialize_f32")
    }

    fn serialize_f64(&mut self, _: f64) -> Result<()> {
        fail!(in self, "Unknown variant does not support serialize_f64")
    }

    fn serialize_bytes(&mut self, _: &[u8]) -> Result<()> {
        fail!(in self, "Unknown variant does not support serialize_bytes")
    }

    fn serialize_str(&mut self, _: &str) -> Result<()> {
        fail!(in self, "Unknown variant does not support serialize_str")
    }

    fn serialize_newtype_variant<V: Serialize + ?Sized>(
        &mut self,
        _: &'static str,
        _: u32,
        _: &'static str,
        _: &V,
    ) -> Result<()> {
        fail!(in self, "Unknown variant does not support serialize_newtype_variant")
    }

    fn serialize_unit_struct(&mut self, _: &'static str) -> Result<()> {
        fail!(in self, "Unknown variant does not support serialize_unit_struct")
    }

    fn serialize_unit_variant(&mut self, _: &'static str, _: u32, _: &'static str) -> Result<()> {
        fail!(in self, "Unknown variant does not support serialize_unit_variant")
    }

    fn serialize_map_start(&mut self, _: Option<usize>) -> Result<()> {
        fail!(in self, "Unknown variant does not support serialize_map_start")
    }

    fn serialize_map_key<V: Serialize + ?Sized>(&mut self, _: &V) -> Result<()> {
        fail!(in self, "Unknown variant does not support serialize_map_jey")
    }

    fn serialize_map_value<V: Serialize + ?Sized>(&mut self, _: &V) -> Result<()> {
        fail!(in self, "Unknown variant does not support serialize_map_value")
    }

    fn serialize_map_end(&mut self) -> Result<()> {
        fail!(in self, "Unknown variant does not support serialize_map_end")
    }

    fn serialize_seq_start(&mut self, _: Option<usize>) -> Result<()> {
        fail!(in self, "Unknown variant does not support serialize_seq_start")
    }

    fn serialize_seq_element<V: Serialize + ?Sized>(&mut self, _: &V) -> Result<()> {
        fail!(in self, "Unknown variant does not support serialize_seq_element")
    }

    fn serialize_seq_end(&mut self) -> Result<()> {
        fail!(in self, "Unknown variant does not support serialize_seq_end")
    }

    fn serialize_struct_start(&mut self, _: &'static str, _: usize) -> Result<()> {
        fail!(in self, "Unknown variant does not support serialize_struct_start")
    }

    fn serialize_struct_field<V: Serialize + ?Sized>(
        &mut self,
        _: &'static str,
        _: &V,
    ) -> Result<()> {
        fail!(in self, "Unknown variant does not support serialize_struct_field")
    }

    fn serialize_struct_end(&mut self) -> Result<()> {
        fail!(in self, "Unknown variant does not support serialize_struct_end")
    }

    fn serialize_tuple_start(&mut self, _: usize) -> Result<()> {
        fail!(in self, "Unknown variant does not support serialize_tuple_start")
    }

    fn serialize_tuple_element<V: Serialize + ?Sized>(&mut self, _: &V) -> Result<()> {
        fail!(in self, "Unknown variant does not support serialize_tuple_element")
    }

    fn serialize_tuple_end(&mut self) -> Result<()> {
        fail!(in self, "Unknown variant does not support serialize_tuple_end")
    }

    fn serialize_tuple_struct_start(&mut self, _: &'static str, _: usize) -> Result<()> {
        fail!(in self, "Unknown variant does not support serialize_tuple_struct_start")
    }

    fn serialize_tuple_struct_field<V: Serialize + ?Sized>(&mut self, _: &V) -> Result<()> {
        fail!(in self, "Unknown variant does not support serialize_tuple_struct_field");
    }

    fn serialize_tuple_struct_end(&mut self) -> Result<()> {
        fail!(in self, "Unknown variant does not support serialize_tuple_struct_end");
    }

    fn serialize_struct_variant_start<'this>(
        &'this mut self,
        _: &'static str,
        _: u32,
        _: &'static str,
        _: usize,
    ) -> Result<&'this mut ArrayBuilder> {
        fail!(in self, "Unknown variant does not support serialize_struct_variant_start")
    }

    fn serialize_tuple_variant_start<'this>(
        &'this mut self,
        _: &'static str,
        _: u32,
        _: &'static str,
        _: usize,
    ) -> Result<&'this mut ArrayBuilder> {
        fail!(in self, "Unknown variant does not support serialize_tuple_variant_start")
    }
}
