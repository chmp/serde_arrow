use std::collections::{BTreeMap, HashMap};

use marrow::{
    array::{Array, PrimitiveArray},
    datatypes::FieldMeta,
};
use serde::{Serialize, Serializer};

use crate::internal::{
    error::{set_default, try_, Context, ContextSupport, Result},
    serialization::utils::impl_serializer,
    utils::{
        array_ext::{ArrayExt, ScalarArrayExt},
        NamedType,
    },
};

use super::array_builder::ArrayBuilder;

pub trait IntType: NamedType + Default + 'static {
    const ARRAY_BUILDER_VARIANT: fn(IntBuilder<Self>) -> ArrayBuilder;
    const ARRAY_VARIANT: fn(PrimitiveArray<Self>) -> Array;

    fn from_u8(v: u8) -> Result<Self>;
    fn from_u16(v: u16) -> Result<Self>;
    fn from_u32(v: u32) -> Result<Self>;
    fn from_u64(v: u64) -> Result<Self>;
    fn from_i8(v: i8) -> Result<Self>;
    fn from_i16(v: i16) -> Result<Self>;
    fn from_i32(v: i32) -> Result<Self>;
    fn from_i64(v: i64) -> Result<Self>;
}

macro_rules! impl_int_type {
    ($ty:ty, $array_builder_variant: ident, $array_variant:ident) => {
        impl IntType for $ty {
            const ARRAY_BUILDER_VARIANT: fn(IntBuilder<Self>) -> ArrayBuilder =
                ArrayBuilder::$array_builder_variant;
            const ARRAY_VARIANT: fn(PrimitiveArray<Self>) -> Array = Array::$array_variant;

            fn from_u8(v: u8) -> Result<Self> {
                Ok(Self::try_from(v)?)
            }

            fn from_u16(v: u16) -> Result<Self> {
                Ok(Self::try_from(v)?)
            }

            fn from_u32(v: u32) -> Result<Self> {
                Ok(Self::try_from(v)?)
            }

            fn from_u64(v: u64) -> Result<Self> {
                Ok(Self::try_from(v)?)
            }

            fn from_i8(v: i8) -> Result<Self> {
                Ok(Self::try_from(v)?)
            }

            fn from_i16(v: i16) -> Result<Self> {
                Ok(Self::try_from(v)?)
            }

            fn from_i32(v: i32) -> Result<Self> {
                Ok(Self::try_from(v)?)
            }

            fn from_i64(v: i64) -> Result<Self> {
                Ok(Self::try_from(v)?)
            }
        }
    };
}

impl_int_type!(i8, I8, Int8);
impl_int_type!(i16, I16, Int16);
impl_int_type!(i32, I32, Int32);
impl_int_type!(i64, I64, Int64);
impl_int_type!(u8, U8, UInt8);
impl_int_type!(u16, U16, UInt16);
impl_int_type!(u32, U32, UInt32);
impl_int_type!(u64, U64, UInt64);

#[derive(Debug, Clone)]
pub struct IntBuilder<I> {
    pub name: String,
    array: PrimitiveArray<I>,
    metadata: HashMap<String, String>,
}

impl<I: IntType> IntBuilder<I> {
    pub fn new(name: String, is_nullable: bool, metadata: HashMap<String, String>) -> Self {
        Self {
            name,
            metadata,
            array: PrimitiveArray::new(is_nullable),
        }
    }

    pub fn is_nullable(&self) -> bool {
        self.array.is_nullable()
    }

    pub fn reserve(&mut self, len: usize) {
        self.array.reserve(len);
    }

    pub fn take(&mut self) -> ArrayBuilder {
        I::ARRAY_BUILDER_VARIANT(Self {
            name: self.name.clone(),
            metadata: self.metadata.clone(),
            array: self.array.take(),
        })
    }

    pub fn into_array_and_field_meta(self) -> Result<(Array, FieldMeta)> {
        let meta = FieldMeta {
            name: self.name,
            metadata: self.metadata,
            nullable: self.array.is_nullable(),
        };
        Ok((I::ARRAY_VARIANT(self.array), meta))
    }

    pub fn serialize_default_value(&mut self) -> Result<()> {
        try_(|| self.array.push_scalar_default()).ctx(self)
    }

    pub fn serialize_value<V: Serialize>(&mut self, value: V) -> Result<()> {
        value.serialize(&mut *self).ctx(self)
    }
}

macro_rules! impl_into_array {
    ($ty:ty, $builder_var: ident, $array_var:ident) => {
        impl IntBuilder<$ty> {}
    };
}

impl_into_array!(i8, I8, Int8);
impl_into_array!(i16, I16, Int16);
impl_into_array!(i32, I32, Int32);
impl_into_array!(i64, I64, Int64);
impl_into_array!(u8, U8, UInt8);
impl_into_array!(u16, U16, UInt16);
impl_into_array!(u32, U32, UInt32);
impl_into_array!(u64, U64, UInt64);

impl<I: NamedType> Context for IntBuilder<I> {
    fn annotate(&self, annotations: &mut BTreeMap<String, String>) {
        set_default(annotations, "field", &self.name);
        set_default(
            annotations,
            "data_type",
            match I::NAME {
                "i8" => "Int8",
                "i16" => "Int16",
                "i32" => "Int32",
                "i64" => "Int64",
                "u8" => "UInt8",
                "u16" => "UInt16",
                "u32" => "UInt32",
                "u64" => "UInt64",
                _ => "<unknown>",
            },
        );
    }
}

impl<'a, I: IntType> Serializer for &'a mut IntBuilder<I> {
    impl_serializer!(
        'a, IntBuilder;
        override serialize_none,
        override serialize_bool,
        override serialize_i8,
        override serialize_i16,
        override serialize_i32,
        override serialize_i64,
        override serialize_u8,
        override serialize_u16,
        override serialize_u32,
        override serialize_u64,
        override serialize_char,
    );

    fn serialize_none(self) -> Result<()> {
        self.array.push_scalar_none()
    }

    fn serialize_bool(self, v: bool) -> Result<()> {
        let v: u8 = if v { 1 } else { 0 };
        self.array.push_scalar_value(I::from_u8(v)?)
    }

    fn serialize_i8(self, v: i8) -> Result<()> {
        self.array.push_scalar_value(I::from_i8(v)?)
    }

    fn serialize_i16(self, v: i16) -> Result<()> {
        self.array.push_scalar_value(I::from_i16(v)?)
    }

    fn serialize_i32(self, v: i32) -> Result<()> {
        self.array.push_scalar_value(I::from_i32(v)?)
    }

    fn serialize_i64(self, v: i64) -> Result<()> {
        self.array.push_scalar_value(I::from_i64(v)?)
    }

    fn serialize_u8(self, v: u8) -> Result<()> {
        self.array.push_scalar_value(I::from_u8(v)?)
    }

    fn serialize_u16(self, v: u16) -> Result<()> {
        self.array.push_scalar_value(I::from_u16(v)?)
    }

    fn serialize_u32(self, v: u32) -> Result<()> {
        self.array.push_scalar_value(I::from_u32(v)?)
    }

    fn serialize_u64(self, v: u64) -> Result<()> {
        self.array.push_scalar_value(I::from_u64(v)?)
    }

    fn serialize_char(self, v: char) -> Result<()> {
        self.array.push_scalar_value(I::from_u32(u32::from(v))?)
    }
}
