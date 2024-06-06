use crate::internal::error::{fail, Result};

use super::utils::{
    push_validity, push_validity_default, MutableBitBuffer, MutableOffsetBuffer, Offset,
    SimpleSerializer,
};

#[derive(Debug, Clone)]
pub struct Utf8Builder<O> {
    pub validity: Option<MutableBitBuffer>,
    pub offsets: MutableOffsetBuffer<O>,
    pub buffer: Vec<u8>,
}

impl<O: Offset> Utf8Builder<O> {
    pub fn new(is_nullable: bool) -> Self {
        Self {
            validity: is_nullable.then(MutableBitBuffer::default),
            offsets: MutableOffsetBuffer::default(),
            buffer: Vec::new(),
        }
    }

    pub fn take(&mut self) -> Self {
        Self {
            validity: self.validity.as_mut().map(std::mem::take),
            offsets: std::mem::take(&mut self.offsets),
            buffer: std::mem::take(&mut self.buffer),
        }
    }

    pub fn is_nullable(&self) -> bool {
        self.validity.is_some()
    }
}

impl<O: Offset> SimpleSerializer for Utf8Builder<O> {
    fn name(&self) -> &str {
        "Utf8Builder"
    }

    fn serialize_default(&mut self) -> Result<()> {
        push_validity_default(&mut self.validity);
        self.offsets.push_current_items();
        Ok(())
    }

    fn serialize_none(&mut self) -> Result<()> {
        push_validity(&mut self.validity, false)?;
        self.offsets.push_current_items();
        Ok(())
    }

    fn serialize_str(&mut self, v: &str) -> Result<()> {
        push_validity(&mut self.validity, true)?;
        self.offsets.push(v.len())?;
        self.buffer.extend(v.as_bytes());

        Ok(())
    }

    fn serialize_unit_variant(
        &mut self,
        _: &'static str,
        _: u32,
        variant: &'static str,
    ) -> Result<()> {
        self.serialize_str(variant)
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
