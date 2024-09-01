use std::collections::BTreeMap;

use serde::Serialize;

use crate::internal::{
    arrow::{Array, NullArray},
    error::{fail, Context, Result},
    utils::btree_map,
};

use super::{simple_serializer::SimpleSerializer, ArrayBuilder};

#[derive(Debug, Clone)]
pub struct UnknownVariantBuilder {
    path: String,
}

impl UnknownVariantBuilder {
    pub fn new(path: String) -> Self {
        UnknownVariantBuilder { path }
    }

    pub fn take(&mut self) -> Self {
        UnknownVariantBuilder {
            path: self.path.clone(),
        }
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
        btree_map!("field" => self.path.clone())
    }
}

impl SimpleSerializer for UnknownVariantBuilder {
    fn serialize_default(&mut self) -> Result<()> {
        fail!(in self, "Serialization failed: an unknown variant")
    }

    fn serialize_unit(&mut self) -> Result<()> {
        fail!(in self, "Serialization failed: an unknown variant")
    }

    fn serialize_none(&mut self) -> Result<()> {
        fail!(in self, "Serialization failed: an unknown variant")
    }

    fn serialize_bool(&mut self, _: bool) -> Result<()> {
        fail!(in self, "Serialization failed: an unknown variant")
    }

    fn serialize_char(&mut self, _: char) -> Result<()> {
        fail!(in self, "Serialization failed: an unknown variant")
    }

    fn serialize_u8(&mut self, _: u8) -> Result<()> {
        fail!(in self, "Serialization failed: an unknown variant")
    }

    fn serialize_u16(&mut self, _: u16) -> Result<()> {
        fail!(in self, "Serialization failed: an unknown variant")
    }

    fn serialize_u32(&mut self, _: u32) -> Result<()> {
        fail!(in self, "Serialization failed: an unknown variant")
    }

    fn serialize_u64(&mut self, _: u64) -> Result<()> {
        fail!(in self, "Serialization failed: an unknown variant")
    }

    fn serialize_i8(&mut self, _: i8) -> Result<()> {
        fail!(in self, "Serialization failed: an unknown variant")
    }

    fn serialize_i16(&mut self, _: i16) -> Result<()> {
        fail!(in self, "Serialization failed: an unknown variant")
    }

    fn serialize_i32(&mut self, _: i32) -> Result<()> {
        fail!(in self, "Serialization failed: an unknown variant")
    }

    fn serialize_i64(&mut self, _: i64) -> Result<()> {
        fail!(in self, "Serialization failed: an unknown variant")
    }

    fn serialize_f32(&mut self, _: f32) -> Result<()> {
        fail!(in self, "Serialization failed: an unknown variant")
    }

    fn serialize_f64(&mut self, _: f64) -> Result<()> {
        fail!(in self, "Serialization failed: an unknown variant")
    }

    fn serialize_bytes(&mut self, _: &[u8]) -> Result<()> {
        fail!(in self, "Serialization failed: an unknown variant")
    }

    fn serialize_str(&mut self, _: &str) -> Result<()> {
        fail!(in self, "Serialization failed: an unknown variant")
    }

    fn serialize_newtype_variant<V: Serialize + ?Sized>(
        &mut self,
        _: &'static str,
        _: u32,
        _: &'static str,
        _: &V,
    ) -> Result<()> {
        fail!(in self, "Serialization failed: an unknown variant")
    }

    fn serialize_unit_struct(&mut self, _: &'static str) -> Result<()> {
        fail!(in self, "Serialization failed: an unknown variant")
    }

    fn serialize_unit_variant(&mut self, _: &'static str, _: u32, _: &'static str) -> Result<()> {
        fail!(in self, "Serialization failed: an unknown variant")
    }

    fn serialize_map_start(&mut self, _: Option<usize>) -> Result<()> {
        fail!(in self, "Serialization failed: an unknown variant")
    }

    fn serialize_map_key<V: Serialize + ?Sized>(&mut self, _: &V) -> Result<()> {
        fail!(in self, "Serialization failed: an unknown variant")
    }

    fn serialize_map_value<V: Serialize + ?Sized>(&mut self, _: &V) -> Result<()> {
        fail!(in self, "Serialization failed: an unknown variant")
    }

    fn serialize_map_end(&mut self) -> Result<()> {
        fail!(in self, "Serialization failed: an unknown variant")
    }

    fn serialize_seq_start(&mut self, _: Option<usize>) -> Result<()> {
        fail!(in self, "Serialization failed: an unknown variant")
    }

    fn serialize_seq_element<V: Serialize + ?Sized>(&mut self, _: &V) -> Result<()> {
        fail!(in self, "Serialization failed: an unknown variant")
    }

    fn serialize_seq_end(&mut self) -> Result<()> {
        fail!(in self, "Serialization failed: an unknown variant")
    }

    fn serialize_struct_start(&mut self, _: &'static str, _: usize) -> Result<()> {
        fail!(in self, "Serialization failed: an unknown variant")
    }

    fn serialize_struct_field<V: Serialize + ?Sized>(
        &mut self,
        _: &'static str,
        _: &V,
    ) -> Result<()> {
        fail!(in self, "Serialization failed: an unknown variant")
    }

    fn serialize_struct_end(&mut self) -> Result<()> {
        fail!(in self, "Serialization failed: an unknown variant")
    }

    fn serialize_tuple_start(&mut self, _: usize) -> Result<()> {
        fail!(in self, "Serialization failed: an unknown variant")
    }

    fn serialize_tuple_element<V: Serialize + ?Sized>(&mut self, _: &V) -> Result<()> {
        fail!(in self, "Serialization failed: an unknown variant")
    }

    fn serialize_tuple_end(&mut self) -> Result<()> {
        fail!(in self, "Serialization failed: an unknown variant")
    }

    fn serialize_tuple_struct_start(&mut self, _: &'static str, _: usize) -> Result<()> {
        fail!(in self, "Serialization failed: an unknown variant")
    }

    fn serialize_tuple_struct_field<V: Serialize + ?Sized>(&mut self, _: &V) -> Result<()> {
        fail!(in self, "Serialization failed: an unknown variant");
    }

    fn serialize_tuple_struct_end(&mut self) -> Result<()> {
        fail!(in self, "Serialization failed: an unknown variant");
    }

    fn serialize_struct_variant_start<'this>(
        &'this mut self,
        _: &'static str,
        _: u32,
        _: &'static str,
        _: usize,
    ) -> Result<&'this mut ArrayBuilder> {
        fail!(in self, "Serialization failed: an unknown variant")
    }

    fn serialize_tuple_variant_start<'this>(
        &'this mut self,
        _: &'static str,
        _: u32,
        _: &'static str,
        _: usize,
    ) -> Result<&'this mut ArrayBuilder> {
        fail!(in self, "Serialization failed: an unknown variant")
    }
}
