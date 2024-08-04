use crate::internal::{
    arrow::{Array, PrimitiveArray},
    error::{Error, Result},
};

use super::{
    array_ext::{new_primitive_array, ArrayExt, ScalarArrayExt},
    simple_serializer::SimpleSerializer,
};

#[derive(Debug, Clone)]
pub struct IntBuilder<I>(PrimitiveArray<I>);

impl<I: Default + 'static> IntBuilder<I> {
    pub fn new(is_nullable: bool) -> Self {
        Self(new_primitive_array(is_nullable))
    }

    pub fn take(&mut self) -> Self {
        Self(self.0.take())
    }

    pub fn is_nullable(&self) -> bool {
        self.0.validity.is_some()
    }
}

macro_rules! impl_into_array {
    ($ty:ty, $var:ident) => {
        impl IntBuilder<$ty> {
            pub fn into_array(self) -> Result<Array> {
                Ok(Array::$var(self.0))
            }
        }
    };
}

impl_into_array!(i8, Int8);
impl_into_array!(i16, Int16);
impl_into_array!(i32, Int32);
impl_into_array!(i64, Int64);
impl_into_array!(u8, UInt8);
impl_into_array!(u16, UInt16);
impl_into_array!(u32, UInt32);
impl_into_array!(u64, UInt64);

impl<I> SimpleSerializer for IntBuilder<I>
where
    I: Default
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
    fn name(&self) -> &str {
        "IntBuilder<()>"
    }

    fn serialize_default(&mut self) -> Result<()> {
        self.0.push_scalar_default()
    }

    fn serialize_none(&mut self) -> Result<()> {
        self.0.push_scalar_none()
    }

    fn serialize_i8(&mut self, v: i8) -> Result<()> {
        self.0.push_scalar_value(I::try_from(v)?)
    }

    fn serialize_i16(&mut self, v: i16) -> Result<()> {
        self.0.push_scalar_value(I::try_from(v)?)
    }

    fn serialize_i32(&mut self, v: i32) -> Result<()> {
        self.0.push_scalar_value(I::try_from(v)?)
    }

    fn serialize_i64(&mut self, v: i64) -> Result<()> {
        self.0.push_scalar_value(I::try_from(v)?)
    }

    fn serialize_u8(&mut self, v: u8) -> Result<()> {
        self.0.push_scalar_value(I::try_from(v)?)
    }

    fn serialize_u16(&mut self, v: u16) -> Result<()> {
        self.0.push_scalar_value(I::try_from(v)?)
    }

    fn serialize_u32(&mut self, v: u32) -> Result<()> {
        self.0.push_scalar_value(I::try_from(v)?)
    }

    fn serialize_u64(&mut self, v: u64) -> Result<()> {
        self.0.push_scalar_value(I::try_from(v)?)
    }

    fn serialize_char(&mut self, v: char) -> Result<()> {
        self.0.push_scalar_value(I::try_from(u32::from(v))?)
    }
}
