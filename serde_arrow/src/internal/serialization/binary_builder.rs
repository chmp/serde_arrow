use std::collections::{BTreeMap, HashMap};

use serde::{Serialize, Serializer};

use marrow::{
    array::{Array, BytesArray, BytesViewArray},
    datatypes::FieldMeta,
};

use crate::internal::{
    error::{set_default, Context, ContextSupport, Error, Result},
    serialization::utils::impl_serializer,
    utils::array_ext::{reserve_to_new_capacity, ArrayExt, ScalarArrayExt, SeqArrayExt},
};

use super::array_builder::ArrayBuilder;

pub trait BinaryBuilderArray:
    ArrayExt + for<'s> ScalarArrayExt<'s, Value = &'s [u8]> + SeqArrayExt + Sized
{
    const DATA_TYPE_NAME: &'static str;
    const ARRAY_BUILDER_VARIANT: fn(BinaryBuilder<Self>) -> ArrayBuilder;
    const ARRAY_VARIANT: fn(Self) -> Array;

    fn push_byte(&mut self, byte: u8);
    fn as_serialize_seq(builder: &mut BinaryBuilder<Self>) -> super::utils::SerializeSeq<'_>;
    fn as_serialize_tuple(builder: &mut BinaryBuilder<Self>) -> super::utils::SerializeTuple<'_>;
    fn reserve_values(&mut self, additional: usize);
}

impl BinaryBuilderArray for BytesArray<i32> {
    const DATA_TYPE_NAME: &'static str = "Binary";
    const ARRAY_BUILDER_VARIANT: fn(BinaryBuilder<Self>) -> ArrayBuilder = ArrayBuilder::Binary;
    const ARRAY_VARIANT: fn(Self) -> Array = Array::Binary;

    fn push_byte(&mut self, byte: u8) {
        self.data.push(byte);
    }

    fn as_serialize_seq(builder: &mut BinaryBuilder<Self>) -> super::utils::SerializeSeq<'_> {
        super::utils::SerializeSeq::Binary(builder)
    }

    fn as_serialize_tuple(builder: &mut BinaryBuilder<Self>) -> super::utils::SerializeTuple<'_> {
        super::utils::SerializeTuple::Binary(builder)
    }

    fn reserve_values(&mut self, additional: usize) {
        reserve_to_new_capacity(&mut self.data, additional);
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

    fn as_serialize_seq(builder: &mut BinaryBuilder<Self>) -> super::utils::SerializeSeq<'_> {
        super::utils::SerializeSeq::LargeBinary(builder)
    }

    fn as_serialize_tuple(builder: &mut BinaryBuilder<Self>) -> super::utils::SerializeTuple<'_> {
        super::utils::SerializeTuple::LargeBinary(builder)
    }

    fn reserve_values(&mut self, additional: usize) {
        reserve_to_new_capacity(&mut self.data, additional);
    }
}

impl BinaryBuilderArray for BytesViewArray {
    const DATA_TYPE_NAME: &'static str = "BinaryView";
    const ARRAY_BUILDER_VARIANT: fn(BinaryBuilder<Self>) -> ArrayBuilder = ArrayBuilder::BinaryView;
    const ARRAY_VARIANT: fn(Self) -> Array = Array::BinaryView;

    fn push_byte(&mut self, byte: u8) {
        self.buffers[0].push(byte);
    }

    fn as_serialize_seq(builder: &mut BinaryBuilder<Self>) -> super::utils::SerializeSeq<'_> {
        super::utils::SerializeSeq::BinaryView(builder)
    }

    fn as_serialize_tuple(builder: &mut BinaryBuilder<Self>) -> super::utils::SerializeTuple<'_> {
        super::utils::SerializeTuple::BinaryView(builder)
    }

    fn reserve_values(&mut self, _additional: usize) {
        // assume already reserved
    }
}

#[derive(Debug, Clone)]

pub struct BinaryBuilder<A> {
    pub name: String,
    metadata: HashMap<String, String>,
    array: A,
}

impl<B: BinaryBuilderArray> BinaryBuilder<B> {
    pub fn new(name: String, is_nullable: bool, metadata: HashMap<String, String>) -> Self {
        Self {
            name,
            array: B::new(is_nullable),
            metadata,
        }
    }

    pub fn is_nullable(&self) -> bool {
        self.array.is_nullable()
    }

    pub fn take(&mut self) -> ArrayBuilder {
        B::ARRAY_BUILDER_VARIANT(Self {
            name: self.name.clone(),
            metadata: self.metadata.clone(),
            array: self.array.take(),
        })
    }

    pub fn into_array_and_field_meta(self) -> Result<(Array, FieldMeta)> {
        let meta = FieldMeta {
            name: self.name,
            nullable: self.array.is_nullable(),
            metadata: self.metadata,
        };
        Ok((B::ARRAY_VARIANT(self.array), meta))
    }

    pub fn reserve(&mut self, additional: usize) {
        self.array.reserve(additional);
    }

    pub fn serialize_default_value(&mut self) -> Result<()> {
        self.array.push_scalar_default().ctx(self)
    }

    pub fn serialize_value<V: Serialize>(&mut self, value: V) -> Result<()> {
        value.serialize(&mut *self).ctx(self)
    }
}

impl<B: BinaryBuilderArray> BinaryBuilder<B> {
    fn start(&mut self) -> Result<()> {
        self.array.start_seq()
    }

    fn element<V: Serialize + ?Sized>(&mut self, value: &V) -> Result<()> {
        let byte = value.serialize(U8Serializer)?;
        self.array.push_byte(byte);
        self.array.push_seq_elements(1)
    }

    fn end(&mut self) -> Result<()> {
        self.array.end_seq()
    }
}

impl<B: BinaryBuilderArray> Context for BinaryBuilder<B> {
    fn annotate(&self, annotations: &mut BTreeMap<String, String>) {
        set_default(annotations, "field", &self.name);
        set_default(annotations, "data_type", B::DATA_TYPE_NAME);
    }
}

impl<'a, B: BinaryBuilderArray> Serializer for &'a mut BinaryBuilder<B> {
    impl_serializer!(
        'a, BinaryBuilder;
        override serialize_none,
        override serialize_seq,
        override serialize_tuple,
        override serialize_bytes,
        override serialize_str,
    );

    fn serialize_none(self) -> Result<()> {
        self.array.push_scalar_none()
    }

    fn serialize_seq(self, len: Option<usize>) -> Result<Self::SerializeSeq> {
        if let Some(len) = len {
            self.array.reserve_values(len);
        }
        self.start()?;
        Ok(B::as_serialize_seq(self))
    }

    fn serialize_tuple(self, len: usize) -> Result<Self::SerializeTuple> {
        self.array.reserve_values(len);
        self.start()?;
        Ok(B::as_serialize_tuple(self))
    }

    fn serialize_bytes(self, v: &[u8]) -> Result<()> {
        self.array.push_scalar_value(v)
    }

    fn serialize_str(self, v: &str) -> Result<()> {
        self.array.push_scalar_value(v.as_bytes())
    }
}

impl<B: BinaryBuilderArray> serde::ser::SerializeSeq for &mut BinaryBuilder<B> {
    type Ok = ();
    type Error = Error;

    fn serialize_element<T: ?Sized + Serialize>(&mut self, value: &T) -> Result<()> {
        self.element(value)
    }

    fn end(self) -> Result<()> {
        BinaryBuilder::end(self)
    }
}

impl<B: BinaryBuilderArray> serde::ser::SerializeTuple for &mut BinaryBuilder<B> {
    type Ok = ();
    type Error = Error;

    fn serialize_element<T: ?Sized + Serialize>(&mut self, value: &T) -> Result<()> {
        self.element(value)
    }

    fn end(self) -> Result<()> {
        BinaryBuilder::end(self)
    }
}

pub struct U8Serializer;

impl serde::Serializer for U8Serializer {
    impl_serializer!(
        'static, U8Serializer;
        override Ok,
        override SerializeStruct,
        override SerializeMap,
        override SerializeTupleVariant,
        override SerializeStructVariant,
        override SerializeTuple,
        override SerializeSeq,
        override SerializeTupleStruct,
        override serialize_u8,
        override serialize_u16,
        override serialize_u32,
        override serialize_u64,
        override serialize_i8,
        override serialize_i16,
        override serialize_i32,
        override serialize_i64,
    );

    type Ok = u8;
    type SerializeStruct = serde::ser::Impossible<Self::Ok, Self::Error>;
    type SerializeSeq = serde::ser::Impossible<Self::Ok, Self::Error>;
    type SerializeTuple = serde::ser::Impossible<Self::Ok, Self::Error>;
    type SerializeTupleStruct = serde::ser::Impossible<Self::Ok, Self::Error>;
    type SerializeMap = serde::ser::Impossible<Self::Ok, Self::Error>;
    type SerializeStructVariant = serde::ser::Impossible<Self::Ok, Self::Error>;
    type SerializeTupleVariant = serde::ser::Impossible<Self::Ok, Self::Error>;

    fn serialize_u8(self, v: u8) -> Result<Self::Ok> {
        Ok(v)
    }

    fn serialize_u16(self, v: u16) -> Result<Self::Ok> {
        self.serialize_u8(v.try_into()?)
    }

    fn serialize_u32(self, v: u32) -> Result<Self::Ok> {
        self.serialize_u8(v.try_into()?)
    }

    fn serialize_u64(self, v: u64) -> Result<Self::Ok> {
        self.serialize_u8(v.try_into()?)
    }

    fn serialize_i8(self, v: i8) -> Result<Self::Ok> {
        self.serialize_u8(v.try_into()?)
    }

    fn serialize_i16(self, v: i16) -> Result<Self::Ok> {
        self.serialize_u8(v.try_into()?)
    }

    fn serialize_i32(self, v: i32) -> Result<Self::Ok> {
        self.serialize_u8(v.try_into()?)
    }

    fn serialize_i64(self, v: i64) -> Result<Self::Ok> {
        self.serialize_u8(v.try_into()?)
    }
}
