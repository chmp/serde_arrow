use crate::{fail, Error, Result};
use arrow::{
    array::{
        ArrayRef, BooleanBuilder, Date64Builder, Float32Builder, Int32Builder, Int8Builder,
        StringBuilder,
    },
    datatypes::DataType,
};
use chrono::NaiveDateTime;
use serde::ser::{Impossible, Serialize, Serializer};

use std::sync::Arc;

const DEFAULT_CAPACITY: usize = 1024;

pub enum ArrayBuilder {
    Bool(BooleanBuilder),
    I8(Int8Builder),
    I32(Int32Builder),
    F32(Float32Builder),
    Utf8(StringBuilder),
    Date64(Date64Builder),
}

macro_rules! dispatch {
    ($obj:ident, $builder:pat => $expr:expr) => {
        match $obj {
            ArrayBuilder::Bool($builder) => $expr,
            ArrayBuilder::I8($builder) => $expr,
            ArrayBuilder::I32($builder) => $expr,
            ArrayBuilder::F32($builder) => $expr,
            ArrayBuilder::Utf8($builder) => $expr,
            ArrayBuilder::Date64($builder) => $expr,
        };
    };
}

impl ArrayBuilder {
    pub fn new(data_type: &DataType) -> Result<Self> {
        let res = match data_type {
            DataType::Boolean => Self::Bool(BooleanBuilder::new(DEFAULT_CAPACITY)),
            DataType::Int8 => Self::I8(Int8Builder::new(DEFAULT_CAPACITY)),
            DataType::Int32 => Self::I32(Int32Builder::new(DEFAULT_CAPACITY)),
            DataType::Float32 => Self::F32(Float32Builder::new(DEFAULT_CAPACITY)),
            DataType::Utf8 => Self::Utf8(StringBuilder::new(DEFAULT_CAPACITY)),
            DataType::Date64 => Self::Date64(Date64Builder::new(DEFAULT_CAPACITY)),
            _ => fail!("Cannot build ArrayBuilder for {}", data_type),
        };
        Ok(res)
    }

    pub fn build(&mut self) -> Result<ArrayRef> {
        let array_ref: ArrayRef = dispatch!(self, builder => Arc::new(builder.finish()));
        Ok(array_ref)
    }

    pub fn append_null(&mut self) -> Result<()> {
        dispatch!(self, builder => builder.append_null()?);
        Ok(())
    }
}

impl ArrayBuilder {
    pub fn append_bool(&mut self, value: bool) -> Result<()> {
        match self {
            Self::Bool(builder) => builder.append_value(value)?,
            _ => fail!("Mismatched type"),
        }
        Ok(())
    }

    pub fn append_i8(&mut self, value: i8) -> Result<()> {
        match self {
            Self::I8(builder) => builder.append_value(value)?,
            _ => fail!("Mismatched type"),
        };
        Ok(())
    }

    pub fn append_i32(&mut self, value: i32) -> Result<()> {
        match self {
            Self::I32(builder) => builder.append_value(value)?,
            _ => fail!("Mismatched type"),
        };
        Ok(())
    }

    pub fn append_f32(&mut self, value: f32) -> Result<()> {
        match self {
            Self::F32(builder) => builder.append_value(value)?,
            _ => fail!("Mismatched type"),
        };
        Ok(())
    }

    pub fn append_utf8(&mut self, data: &str) -> Result<()> {
        match self {
            Self::Utf8(builder) => builder.append_value(data)?,
            Self::Date64(builder) => {
                let dt = data.parse::<NaiveDateTime>()?;
                builder.append_value(dt.timestamp_millis())?;
            }
            _ => fail!("Mismatched type"),
        };
        Ok(())
    }
}

macro_rules! unsupported {
    ($name:ident, $ty:ty) => {
        fn $name(self, _v: $ty) -> Result<()> {
            return Err(Error::Custom(String::from("Not supported")));
        }
    };
}

impl<'a> Serializer for &'a mut ArrayBuilder {
    type Ok = ();
    type Error = Error;

    type SerializeSeq = Impossible<Self::Ok, Self::Error>;
    type SerializeStruct = Impossible<Self::Ok, Self::Error>;
    type SerializeTuple = Impossible<Self::Ok, Self::Error>;
    type SerializeTupleStruct = Impossible<Self::Ok, Self::Error>;
    type SerializeTupleVariant = Impossible<Self::Ok, Self::Error>;
    type SerializeMap = Impossible<Self::Ok, Self::Error>;
    type SerializeStructVariant = Impossible<Self::Ok, Self::Error>;

    fn serialize_bool(self, value: bool) -> Result<Self::Ok> {
        self.append_bool(value)
    }

    fn serialize_i8(self, value: i8) -> Result<Self::Ok> {
        self.append_i8(value)
    }

    unsupported!(serialize_i16, i16);

    fn serialize_i32(self, value: i32) -> Result<Self::Ok> {
        self.append_i32(value)
    }

    unsupported!(serialize_i64, i64);
    unsupported!(serialize_u8, u8);
    unsupported!(serialize_u16, u16);
    unsupported!(serialize_u32, u32);
    unsupported!(serialize_u64, u64);

    fn serialize_f32(self, value: f32) -> Result<Self::Ok> {
        self.append_f32(value)
    }

    unsupported!(serialize_f64, f64);
    unsupported!(serialize_char, char);

    fn serialize_str(self, value: &str) -> Result<Self::Ok> {
        self.append_utf8(value)
    }

    unsupported!(serialize_bytes, &[u8]);

    fn serialize_none(self) -> Result<Self::Ok> {
        self.append_null()
    }

    fn serialize_some<T: ?Sized + Serialize>(self, value: &T) -> Result<Self::Ok> {
        value.serialize(self)
    }

    fn serialize_unit(self) -> Result<Self::Ok> {
        fail!("serialize_unit not supported");
    }

    fn serialize_unit_struct(self, _name: &'static str) -> Result<Self::Ok> {
        fail!("serialize_unit_struct not supported");
    }

    fn serialize_unit_variant(
        self,
        _name: &'static str,
        _variant_index: u32,
        _variant: &'static str,
    ) -> Result<()> {
        fail!("serialize_unit_variant not supported");
    }

    fn serialize_newtype_struct<T: ?Sized + Serialize>(
        self,
        _name: &'static str,
        _value: &T,
    ) -> Result<()> {
        fail!("serialize_newtype_struct not supported");
    }

    fn serialize_newtype_variant<T: ?Sized + Serialize>(
        self,
        _name: &'static str,
        _variant_index: u32,
        _variant: &'static str,
        _value: &T,
    ) -> Result<()> {
        fail!("serialize_newtype_variant not supported");
    }

    fn serialize_seq(self, _len: Option<usize>) -> Result<Self::SerializeSeq> {
        fail!("serialize_seq not supported");
    }

    fn serialize_tuple(self, _len: usize) -> Result<Self::SerializeTuple> {
        fail!("serialize_tuple not supported");
    }

    fn serialize_tuple_struct(
        self,
        _name: &'static str,
        _len: usize,
    ) -> Result<Self::SerializeTupleStruct> {
        fail!("serialize_tuple_struct not supported");
    }

    fn serialize_tuple_variant(
        self,
        _name: &'static str,
        _variant_index: u32,
        _variant: &'static str,
        _len: usize,
    ) -> Result<Self::SerializeTupleVariant> {
        fail!("serialize_tuple_variant not supported");
    }

    fn serialize_map(self, _len: Option<usize>) -> Result<Self::SerializeMap> {
        fail!("serialize_map not supported");
    }

    fn serialize_struct(self, _name: &'static str, _len: usize) -> Result<Self::SerializeStruct> {
        fail!("serialize_struct not supported");
    }

    fn serialize_struct_variant(
        self,
        _name: &'static str,
        _variant_index: u32,
        _variant: &'static str,
        _len: usize,
    ) -> Result<Self::SerializeStructVariant> {
        fail!("serialize_struct_variant not supported");
    }
}
