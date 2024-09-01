use std::collections::BTreeMap;

use serde::Serialize;

use crate::internal::{
    arrow::{Array, BytesArray},
    error::{Context, ContextSupport, Result},
    utils::{
        array_ext::{new_bytes_array, ArrayExt, ScalarArrayExt, SeqArrayExt},
        btree_map, Mut, Offset,
    },
};

use super::simple_serializer::SimpleSerializer;

#[derive(Debug, Clone)]

pub struct BinaryBuilder<O> {
    path: String,
    array: BytesArray<O>,
}

impl<O: Offset> BinaryBuilder<O> {
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

impl BinaryBuilder<i32> {
    pub fn into_array(self) -> Result<Array> {
        Ok(Array::Binary(self.array))
    }
}

impl BinaryBuilder<i64> {
    pub fn into_array(self) -> Result<Array> {
        Ok(Array::LargeBinary(self.array))
    }
}

impl<O: Offset> BinaryBuilder<O> {
    fn start(&mut self) -> Result<()> {
        self.array.start_seq()
    }

    fn element<V: Serialize + ?Sized>(&mut self, value: &V) -> Result<()> {
        let mut u8_serializer = U8Serializer(0);
        value.serialize(Mut(&mut u8_serializer))?;

        self.array.data.push(u8_serializer.0);
        self.array.push_seq_elements(1)
    }

    fn end(&mut self) -> Result<()> {
        Ok(())
    }
}

impl<O: Offset> Context for BinaryBuilder<O> {
    fn annotations(&self) -> std::collections::BTreeMap<String, String> {
        btree_map!("field" => self.path.clone())
    }
}

impl<O: Offset> SimpleSerializer for BinaryBuilder<O> {
    fn serialize_default(&mut self) -> Result<()> {
        self.array.push_scalar_default().ctx(self)
    }

    fn serialize_none(&mut self) -> Result<()> {
        self.array.push_scalar_none().ctx(self)
    }

    fn serialize_seq_start(&mut self, _: Option<usize>) -> Result<()> {
        self.start().ctx(self)
    }

    fn serialize_seq_element<V: Serialize + ?Sized>(&mut self, value: &V) -> Result<()> {
        self.element(value).ctx(self)
    }

    fn serialize_seq_end(&mut self) -> Result<()> {
        self.end().ctx(self)
    }

    fn serialize_tuple_start(&mut self, _: usize) -> Result<()> {
        self.start().ctx(self)
    }

    fn serialize_tuple_element<V: Serialize + ?Sized>(&mut self, value: &V) -> Result<()> {
        self.element(value).ctx(self)
    }

    fn serialize_tuple_end(&mut self) -> Result<()> {
        self.end().ctx(self)
    }

    fn serialize_tuple_struct_start(&mut self, _: &'static str, _: usize) -> Result<()> {
        self.start().ctx(self)
    }

    fn serialize_tuple_struct_field<V: Serialize + ?Sized>(&mut self, value: &V) -> Result<()> {
        self.element(value).ctx(self)
    }

    fn serialize_tuple_struct_end(&mut self) -> Result<()> {
        self.end().ctx(self)
    }

    fn serialize_bytes(&mut self, v: &[u8]) -> Result<()> {
        self.array.push_scalar_value(v).ctx(self)
    }
}

struct U8Serializer(u8);

impl Context for U8Serializer {
    fn annotations(&self) -> BTreeMap<String, String> {
        Default::default()
    }
}

impl SimpleSerializer for U8Serializer {
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
