use std::collections::BTreeMap;

use crate::internal::{
    arrow::{Array, PrimitiveArray},
    error::{set_default, try_, Context, ContextSupport, Error, Result},
    utils::{
        array_ext::{new_primitive_array, ArrayExt, ScalarArrayExt},
        NamedType,
    },
};

use super::{array_builder::ArrayBuilder, simple_serializer::SimpleSerializer};

#[derive(Debug, Clone)]
pub struct IntBuilder<I> {
    path: String,
    array: PrimitiveArray<I>,
}

impl<I: Default + 'static> IntBuilder<I> {
    pub fn new(path: String, is_nullable: bool) -> Self {
        Self {
            path,
            array: new_primitive_array(is_nullable),
        }
    }

    pub fn take_self(&mut self) -> Self {
        Self {
            path: self.path.clone(),
            array: self.array.take(),
        }
    }

    pub fn is_nullable(&self) -> bool {
        self.array.validity.is_some()
    }
}

macro_rules! impl_into_array {
    ($ty:ty, $builder_var: ident, $array_var:ident) => {
        impl IntBuilder<$ty> {
            pub fn take(&mut self) -> ArrayBuilder {
                ArrayBuilder::$builder_var(self.take_self())
            }

            pub fn into_array(self) -> Result<Array> {
                Ok(Array::$array_var(self.array))
            }
        }
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
        set_default(annotations, "field", &self.path);
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

impl<I> SimpleSerializer for IntBuilder<I>
where
    I: NamedType
        + Default
        + TryFrom<i8>
        + TryFrom<i16>
        + TryFrom<i32>
        + TryFrom<i64>
        + TryFrom<u8>
        + TryFrom<u16>
        + TryFrom<u32>
        + TryFrom<u64>
        + 'static,
    Error: From<<I as TryFrom<i8>>::Error>,
    Error: From<<I as TryFrom<i16>>::Error>,
    Error: From<<I as TryFrom<i32>>::Error>,
    Error: From<<I as TryFrom<i64>>::Error>,
    Error: From<<I as TryFrom<u8>>::Error>,
    Error: From<<I as TryFrom<u16>>::Error>,
    Error: From<<I as TryFrom<u32>>::Error>,
    Error: From<<I as TryFrom<u64>>::Error>,
{
    fn serialize_default(&mut self) -> Result<()> {
        try_(|| self.array.push_scalar_default()).ctx(self)
    }

    fn serialize_none(&mut self) -> Result<()> {
        try_(|| self.array.push_scalar_none()).ctx(self)
    }

    fn serialize_bool(&mut self, v: bool) -> Result<()> {
        try_(|| {
            let v: u8 = if v { 1 } else { 0 };
            self.array.push_scalar_value(I::try_from(v)?)
        })
        .ctx(self)
    }

    fn serialize_i8(&mut self, v: i8) -> Result<()> {
        try_(|| self.array.push_scalar_value(I::try_from(v)?)).ctx(self)
    }

    fn serialize_i16(&mut self, v: i16) -> Result<()> {
        try_(|| self.array.push_scalar_value(I::try_from(v)?)).ctx(self)
    }

    fn serialize_i32(&mut self, v: i32) -> Result<()> {
        try_(|| self.array.push_scalar_value(I::try_from(v)?)).ctx(self)
    }

    fn serialize_i64(&mut self, v: i64) -> Result<()> {
        try_(|| self.array.push_scalar_value(I::try_from(v)?)).ctx(self)
    }

    fn serialize_u8(&mut self, v: u8) -> Result<()> {
        try_(|| self.array.push_scalar_value(I::try_from(v)?)).ctx(self)
    }

    fn serialize_u16(&mut self, v: u16) -> Result<()> {
        try_(|| self.array.push_scalar_value(I::try_from(v)?)).ctx(self)
    }

    fn serialize_u32(&mut self, v: u32) -> Result<()> {
        try_(|| self.array.push_scalar_value(I::try_from(v)?)).ctx(self)
    }

    fn serialize_u64(&mut self, v: u64) -> Result<()> {
        try_(|| self.array.push_scalar_value(I::try_from(v)?)).ctx(self)
    }

    fn serialize_char(&mut self, v: char) -> Result<()> {
        try_(|| self.array.push_scalar_value(I::try_from(u32::from(v))?)).ctx(self)
    }
}
