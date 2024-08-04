use serde::Serialize;

use crate::internal::{
    arrow::{Array, BytesArray},
    error::Result,
    utils::{Mut, Offset},
};

use super::{
    array_ext::{new_bytes_array, ArrayExt, ScalarArrayExt, SeqArrayExt},
    simple_serializer::SimpleSerializer,
};

#[derive(Debug, Clone)]

pub struct BinaryBuilder<O>(BytesArray<O>);

impl<O: Offset> BinaryBuilder<O> {
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

impl BinaryBuilder<i32> {
    pub fn into_array(self) -> Result<Array> {
        Ok(Array::Binary(self.0))
    }
}

impl BinaryBuilder<i64> {
    pub fn into_array(self) -> Result<Array> {
        Ok(Array::LargeBinary(self.0))
    }
}

impl<O: Offset> BinaryBuilder<O> {
    fn start(&mut self) -> Result<()> {
        self.0.start_seq()
    }

    fn element<V: Serialize + ?Sized>(&mut self, value: &V) -> Result<()> {
        let mut u8_serializer = U8Serializer(0);
        value.serialize(Mut(&mut u8_serializer))?;

        self.0.data.push(u8_serializer.0);
        self.0.push_seq_elements(1)
    }

    fn end(&mut self) -> Result<()> {
        Ok(())
    }
}

impl<O: Offset> SimpleSerializer for BinaryBuilder<O> {
    fn name(&self) -> &str {
        "BinaryBuilder"
    }

    fn serialize_default(&mut self) -> Result<()> {
        self.0.push_scalar_default()
    }

    fn serialize_none(&mut self) -> Result<()> {
        self.0.push_scalar_none()
    }

    fn serialize_seq_start(&mut self, _: Option<usize>) -> Result<()> {
        self.start()
    }

    fn serialize_seq_element<V: Serialize + ?Sized>(&mut self, value: &V) -> Result<()> {
        self.element(value)
    }

    fn serialize_seq_end(&mut self) -> Result<()> {
        self.end()
    }

    fn serialize_tuple_start(&mut self, _: usize) -> Result<()> {
        self.start()
    }

    fn serialize_tuple_element<V: Serialize + ?Sized>(&mut self, value: &V) -> Result<()> {
        self.element(value)
    }

    fn serialize_tuple_end(&mut self) -> Result<()> {
        self.end()
    }

    fn serialize_tuple_struct_start(&mut self, _: &'static str, _: usize) -> Result<()> {
        self.start()
    }

    fn serialize_tuple_struct_field<V: Serialize + ?Sized>(&mut self, value: &V) -> Result<()> {
        self.element(value)
    }

    fn serialize_tuple_struct_end(&mut self) -> Result<()> {
        self.end()
    }

    fn serialize_bytes(&mut self, v: &[u8]) -> Result<()> {
        self.0.push_scalar_value(v)
    }
}

struct U8Serializer(u8);

impl SimpleSerializer for U8Serializer {
    fn name(&self) -> &str {
        "SerializeU8"
    }

    fn serialize_u8(&mut self, v: u8) -> Result<()> {
        self.0 = v;
        Ok(())
    }

    fn serialize_u16(&mut self, v: u16) -> Result<()> {
        self.serialize_u8(v.try_into()?)
    }

    fn serialize_u32(&mut self, v: u32) -> Result<()> {
        self.serialize_u8(v.try_into()?)
    }

    fn serialize_u64(&mut self, v: u64) -> Result<()> {
        self.serialize_u8(v.try_into()?)
    }

    fn serialize_i8(&mut self, v: i8) -> Result<()> {
        self.serialize_u8(v.try_into()?)
    }

    fn serialize_i16(&mut self, v: i16) -> Result<()> {
        self.serialize_u8(v.try_into()?)
    }

    fn serialize_i32(&mut self, v: i32) -> Result<()> {
        self.serialize_u8(v.try_into()?)
    }

    fn serialize_i64(&mut self, v: i64) -> Result<()> {
        self.serialize_u8(v.try_into()?)
    }
}
