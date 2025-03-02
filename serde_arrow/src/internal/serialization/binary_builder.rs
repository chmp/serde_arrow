use std::collections::BTreeMap;

use serde::Serialize;

use marrow::array::{Array, BytesArray, BytesViewArray};

use crate::internal::{
    error::{set_default, Context, ContextSupport, Result},
    utils::{
        array_ext::{ArrayExt, ScalarArrayExt, SeqArrayExt},
        Mut,
    },
};

use super::{array_builder::ArrayBuilder, simple_serializer::SimpleSerializer};

pub trait BinaryBuilderArray:
    ArrayExt + for<'s> ScalarArrayExt<'s, Value = &'s [u8]> + SeqArrayExt + Sized
{
    const DATA_TYPE_NAME: &'static str;
    const ARRAY_BUILDER_VARIANT: fn(BinaryBuilder<Self>) -> ArrayBuilder;
    const ARRAY_VARIANT: fn(Self) -> Array;

    fn push_byte(&mut self, byte: u8);
}

impl BinaryBuilderArray for BytesArray<i32> {
    const DATA_TYPE_NAME: &'static str = "Binary";
    const ARRAY_BUILDER_VARIANT: fn(BinaryBuilder<Self>) -> ArrayBuilder = ArrayBuilder::Binary;
    const ARRAY_VARIANT: fn(Self) -> Array = Array::Binary;

    fn push_byte(&mut self, byte: u8) {
        self.data.push(byte);
    }
}

impl BinaryBuilderArray for BytesArray<i64> {
    const DATA_TYPE_NAME: &'static str = "LargeBinary";
    const ARRAY_BUILDER_VARIANT: fn(BinaryBuilder<Self>) -> ArrayBuilder =
        ArrayBuilder::LargeBinary;
    const ARRAY_VARIANT: fn(Self) -> Array = Array::LargeBinary;

    fn push_byte(&mut self, byte: u8) {
        self.data.push(byte);
    }
}

impl BinaryBuilderArray for BytesViewArray {
    const DATA_TYPE_NAME: &'static str = "BinaryView";
    const ARRAY_BUILDER_VARIANT: fn(BinaryBuilder<Self>) -> ArrayBuilder = ArrayBuilder::BinaryView;
    const ARRAY_VARIANT: fn(Self) -> Array = Array::BinaryView;

    fn push_byte(&mut self, byte: u8) {
        self.buffers[0].push(byte);
    }
}

#[derive(Debug, Clone)]

pub struct BinaryBuilder<A> {
    path: String,
    array: A,
}

impl<B: BinaryBuilderArray> BinaryBuilder<B> {
    pub fn new(path: String, is_nullable: bool) -> Self {
        Self {
            path,
            array: B::new(is_nullable),
        }
    }

    pub fn take_self(&mut self) -> Self {
        Self {
            path: self.path.clone(),
            array: self.array.take(),
        }
    }

    pub fn is_nullable(&self) -> bool {
        self.array.is_nullable()
    }

    pub fn take(&mut self) -> ArrayBuilder {
        B::ARRAY_BUILDER_VARIANT(self.take_self())
    }

    pub fn into_array(self) -> Result<Array> {
        Ok(B::ARRAY_VARIANT(self.array))
    }
}

impl<B: BinaryBuilderArray> BinaryBuilder<B> {
    fn start(&mut self) -> Result<()> {
        self.array.start_seq()
    }

    fn element<V: Serialize + ?Sized>(&mut self, value: &V) -> Result<()> {
        let mut u8_serializer = U8Serializer(0);
        value.serialize(Mut(&mut u8_serializer))?;

        self.array.push_byte(u8_serializer.0);
        self.array.push_seq_elements(1)
    }

    fn end(&mut self) -> Result<()> {
        self.array.end_seq()
    }
}

impl<B: BinaryBuilderArray> Context for BinaryBuilder<B> {
    fn annotate(&self, annotations: &mut BTreeMap<String, String>) {
        set_default(annotations, "field", &self.path);
        set_default(annotations, "data_type", B::DATA_TYPE_NAME);
    }
}

impl<B: BinaryBuilderArray> SimpleSerializer for BinaryBuilder<B> {
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
    fn annotate(&self, _: &mut BTreeMap<String, String>) {}
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
