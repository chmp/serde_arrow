use serde::Serialize;

use crate::{internal::error::fail, Result};

use super::{utils::SimpleSerializer, ArrayBuilder};

#[derive(Debug, Clone)]
pub struct UnknownVariantBuilder;

impl UnknownVariantBuilder {
    pub fn take(&mut self) -> Self {
        UnknownVariantBuilder
    }

    pub fn is_nullable(&self) -> bool {
        false
    }

    pub fn reserve(&mut self, _num_elements: usize) -> Result<()> {
        Ok(())
    }
}

impl SimpleSerializer for UnknownVariantBuilder {
    fn name(&self) -> &str {
        "UnknownVariantBuilder"
    }

    fn serialize_default(&mut self) -> Result<()> {
        fail!("Serialization failed: an unknown variant")
    }

    fn serialize_unit(&mut self) -> Result<()> {
        fail!("Serialization failed: an unknown variant")
    }

    fn serialize_none(&mut self) -> Result<()> {
        fail!("Serialization failed: an unknown variant")
    }

    fn serialize_bool(&mut self, _: bool) -> Result<()> {
        fail!("Serialization failed: an unknown variant")
    }

    fn serialize_char(&mut self, _: char) -> Result<()> {
        fail!("Serialization failed: an unknown variant")
    }

    fn serialize_u8(&mut self, _: u8) -> Result<()> {
        fail!("Serialization failed: an unknown variant")
    }

    fn serialize_u16(&mut self, _: u16) -> Result<()> {
        fail!("Serialization failed: an unknown variant")
    }

    fn serialize_u32(&mut self, _: u32) -> Result<()> {
        fail!("Serialization failed: an unknown variant")
    }

    fn serialize_u64(&mut self, _: u64) -> Result<()> {
        fail!("Serialization failed: an unknown variant")
    }

    fn serialize_i8(&mut self, _: i8) -> Result<()> {
        fail!("Serialization failed: an unknown variant")
    }

    fn serialize_i16(&mut self, _: i16) -> Result<()> {
        fail!("Serialization failed: an unknown variant")
    }

    fn serialize_i32(&mut self, _: i32) -> Result<()> {
        fail!("Serialization failed: an unknown variant")
    }

    fn serialize_i64(&mut self, _: i64) -> Result<()> {
        fail!("Serialization failed: an unknown variant")
    }

    fn serialize_f32(&mut self, _: f32) -> Result<()> {
        fail!("Serialization failed: an unknown variant")
    }

    fn serialize_f64(&mut self, _: f64) -> Result<()> {
        fail!("Serialization failed: an unknown variant")
    }

    fn serialize_bytes(&mut self, _: &[u8]) -> Result<()> {
        fail!("Serialization failed: an unknown variant")
    }

    fn serialize_str(&mut self, _: &str) -> Result<()> {
        fail!("Serialization failed: an unknown variant")
    }

    fn serialize_newtype_variant<V: Serialize + ?Sized>(
        &mut self,
        _: &'static str,
        _: u32,
        _: &'static str,
        _: &V,
    ) -> Result<()> {
        fail!("Serialization failed: an unknown variant")
    }

    fn serialize_unit_struct(&mut self, _: &'static str) -> Result<()> {
        fail!("Serialization failed: an unknown variant")
    }

    fn serialize_unit_variant(&mut self, _: &'static str, _: u32, _: &'static str) -> Result<()> {
        fail!("Serialization failed: an unknown variant")
    }

    fn serialize_map_start(&mut self, _: Option<usize>) -> Result<()> {
        fail!("Serialization failed: an unknown variant")
    }

    fn serialize_map_key<V: Serialize + ?Sized>(&mut self, _: &V) -> Result<()> {
        fail!("Serialization failed: an unknown variant")
    }

    fn serialize_map_value<V: Serialize + ?Sized>(&mut self, _: &V) -> Result<()> {
        fail!("Serialization failed: an unknown variant")
    }

    fn serialize_map_end(&mut self) -> Result<()> {
        fail!("Serialization failed: an unknown variant")
    }

    fn serialize_seq_start(&mut self, _: Option<usize>) -> Result<()> {
        fail!("Serialization failed: an unknown variant")
    }

    fn serialize_seq_element<V: Serialize + ?Sized>(&mut self, _: &V) -> Result<()> {
        fail!("Serialization failed: an unknown variant")
    }

    fn serialize_seq_end(&mut self) -> Result<()> {
        fail!("Serialization failed: an unknown variant")
    }

    fn serialize_struct_start(&mut self, _: &'static str, _: usize) -> Result<()> {
        fail!("Serialization failed: an unknown variant")
    }

    fn serialize_struct_field<V: Serialize + ?Sized>(
        &mut self,
        _: &'static str,
        _: &V,
    ) -> Result<()> {
        fail!("Serialization failed: an unknown variant")
    }

    fn serialize_struct_end(&mut self) -> Result<()> {
        fail!("Serialization failed: an unknown variant")
    }

    fn serialize_tuple_start(&mut self, _: usize) -> Result<()> {
        fail!("Serialization failed: an unknown variant")
    }

    fn serialize_tuple_element<V: Serialize + ?Sized>(&mut self, _: &V) -> Result<()> {
        fail!("Serialization failed: an unknown variant")
    }

    fn serialize_tuple_end(&mut self) -> Result<()> {
        fail!("Serialization failed: an unknown variant")
    }

    fn serialize_tuple_struct_start(&mut self, _: &'static str, _: usize) -> Result<()> {
        fail!("Serialization failed: an unknown variant")
    }

    fn serialize_tuple_struct_field<V: Serialize + ?Sized>(&mut self, _: &V) -> Result<()> {
        fail!("Serialization failed: an unknown variant");
    }

    fn serialize_tuple_struct_end(&mut self) -> Result<()> {
        fail!("Serialization failed: an unknown variant");
    }

    fn serialize_struct_variant_start<'this>(
        &'this mut self,
        _: &'static str,
        _: u32,
        _: &'static str,
        _: usize,
    ) -> Result<&'this mut ArrayBuilder> {
        fail!("Serialization failed: an unknown variant")
    }

    fn serialize_tuple_variant_start<'this>(
        &'this mut self,
        _: &'static str,
        _: u32,
        _: &'static str,
        _: usize,
    ) -> Result<&'this mut ArrayBuilder> {
        fail!("Serialization failed: an unknown variant")
    }
}
