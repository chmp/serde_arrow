use serde::{ser::SerializeSeq, Serialize};

use crate::{
    internal::{common::MutableOffsetBuffer, error::fail},
    Error, Result,
};

use super::{array_builder::ArrayBuilder, not_implemented::NotImplemented};

#[derive(Debug, Clone)]

pub struct ListBuilder {
    pub offsets: MutableOffsetBuffer<i32>,
    pub element: Box<ArrayBuilder>,
}

impl ListBuilder {
    pub fn new(element: ArrayBuilder) -> Self {
        Self {
            offsets: Default::default(),
            element: Box::new(element),
        }
    }
}

impl<'a> serde::Serializer for &'a mut ListBuilder {
    type Error = Error;
    type Ok = ();

    type SerializeMap = NotImplemented;
    type SerializeSeq = &'a mut ListBuilder;
    type SerializeStruct = NotImplemented;
    type SerializeTuple = NotImplemented;
    type SerializeStructVariant = NotImplemented;
    type SerializeTupleStruct = NotImplemented;
    type SerializeTupleVariant = NotImplemented;

    fn serialize_unit(self) -> Result<()> {
        fail!("Serializer::serialize_unit is not implemented")
    }

    fn serialize_none(self) -> Result<()> {
        fail!("Serializer::serialize_none is not implemented")
    }

    fn serialize_some<T: Serialize + ?Sized>(self, _: &T) -> Result<()> {
        fail!("Serializer::serialize_some is not implemented")
    }

    fn serialize_bool(self, _: bool) -> Result<()> {
        fail!("Serializer::serialize_bool is not implemented")
    }

    fn serialize_char(self, _: char) -> Result<()> {
        fail!("Serializer::serialize_char is not implemented")
    }

    fn serialize_u8(self, _: u8) -> Result<()> {
        fail!("Serializer::serialize_u8 is not implemented")
    }

    fn serialize_u16(self, _: u16) -> Result<()> {
        fail!("Serializer::serialize_u16 is not implemented")
    }

    fn serialize_u32(self, _: u32) -> Result<()> {
        fail!("Serializer::serialize_u32 is not implemented")
    }

    fn serialize_u64(self, _: u64) -> Result<()> {
        fail!("Serializer::serialize_u64 is not implemented")
    }

    fn serialize_i8(self, _: i8) -> Result<()> {
        fail!("Serializer::serialize_i8 is not implemented")
    }

    fn serialize_i16(self, _: i16) -> Result<()> {
        fail!("Serializer::serialize_i16 is not implemented")
    }

    fn serialize_i32(self, _: i32) -> Result<()> {
        fail!("Serializer::serialize_i32 is not implemented")
    }

    fn serialize_i64(self, _: i64) -> Result<()> {
        fail!("Serializer::serialize_i64 is not implemented")
    }

    fn serialize_f32(self, _: f32) -> Result<()> {
        fail!("Serializer::serialize_f32 is not implemented")
    }

    fn serialize_f64(self, _: f64) -> Result<()> {
        fail!("Serializer::serialize_f64 is not implemented")
    }

    fn serialize_bytes(self, _: &[u8]) -> Result<()> {
        fail!("Serializer::serialize_bytes is not implemented")
    }

    fn serialize_str(self, _: &str) -> Result<()> {
        fail!("Serializer::serialize_str is not implemented")
    }

    fn serialize_map(self, _: Option<usize>) -> Result<Self::SerializeMap> {
        Ok(NotImplemented)
    }

    fn serialize_seq(self, _: Option<usize>) -> Result<Self::SerializeSeq> {
        Ok(&mut *self)
    }

    fn serialize_struct(self, _: &'static str, _: usize) -> Result<Self::SerializeStruct> {
        Ok(NotImplemented)
    }

    fn serialize_tuple(self, _: usize) -> Result<Self::SerializeTuple> {
        Ok(NotImplemented)
    }

    fn serialize_newtype_struct<T: Serialize + ?Sized>(self, _: &'static str, _: &T) -> Result<()> {
        fail!("Serializer::serialize_newtype_struct is not implemented")
    }

    fn serialize_newtype_variant<T: Serialize + ?Sized>(
        self,
        _: &'static str,
        _: u32,
        _: &'static str,
        _: &T,
    ) -> Result<()> {
        fail!("Serializer::serialize_newtype_variant is not implemented")
    }

    fn serialize_unit_struct(self, _: &'static str) -> Result<()> {
        fail!("Serializer::serialize_unit_struct is not implemented")
    }

    fn serialize_unit_variant(self, _: &'static str, _: u32, _: &'static str) -> Result<()> {
        fail!("Serializer::serialize_unit_variant is not implemented")
    }

    fn serialize_struct_variant(
        self,
        _: &'static str,
        _: u32,
        _: &'static str,
        _: usize,
    ) -> Result<Self::SerializeStructVariant> {
        Ok(NotImplemented)
    }

    fn serialize_tuple_struct(
        self,
        _: &'static str,
        _: usize,
    ) -> Result<Self::SerializeTupleStruct> {
        Ok(NotImplemented)
    }

    fn serialize_tuple_variant(
        self,
        _: &'static str,
        _: u32,
        _: &'static str,
        _: usize,
    ) -> Result<Self::SerializeTupleVariant> {
        Ok(NotImplemented)
    }
}

impl<'a> SerializeSeq for &'a mut ListBuilder {
    type Ok = ();
    type Error = Error;

    fn serialize_element<T: Serialize + ?Sized>(&mut self, value: &T) -> Result<()> {
        self.offsets.inc_current_items()?;
        value.serialize(self.element.as_mut())?;
        Ok(())
    }

    fn end(self) -> Result<()> {
        self.offsets.push_current_items();
        Ok(())
    }
}
