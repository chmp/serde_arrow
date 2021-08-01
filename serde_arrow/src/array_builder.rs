use crate::{fail, Error, Result};
use arrow::{
    array::{
        ArrayRef, BooleanBuilder, Date64Builder, Float32Builder, Float64Builder, Int16Builder,
        Int32Builder, Int64Builder, Int8Builder, LargeStringBuilder, StringBuilder, UInt16Builder,
        UInt32Builder, UInt64Builder, UInt8Builder,
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
    I16(Int16Builder),
    I32(Int32Builder),
    I64(Int64Builder),
    U8(UInt8Builder),
    U16(UInt16Builder),
    U32(UInt32Builder),
    U64(UInt64Builder),
    F32(Float32Builder),
    F64(Float64Builder),
    Utf8(StringBuilder),
    LargeUtf8(LargeStringBuilder),
    Date64(Date64Builder),
}

macro_rules! dispatch {
    ($obj:ident, $builder:pat => $expr:expr) => {
        match $obj {
            ArrayBuilder::Bool($builder) => $expr,
            ArrayBuilder::I8($builder) => $expr,
            ArrayBuilder::I16($builder) => $expr,
            ArrayBuilder::I32($builder) => $expr,
            ArrayBuilder::I64($builder) => $expr,
            ArrayBuilder::U8($builder) => $expr,
            ArrayBuilder::U16($builder) => $expr,
            ArrayBuilder::U32($builder) => $expr,
            ArrayBuilder::U64($builder) => $expr,
            ArrayBuilder::F32($builder) => $expr,
            ArrayBuilder::F64($builder) => $expr,
            ArrayBuilder::Utf8($builder) => $expr,
            ArrayBuilder::LargeUtf8($builder) => $expr,
            ArrayBuilder::Date64($builder) => $expr,
        };
    };
}

impl ArrayBuilder {
    pub fn new(data_type: &DataType) -> Result<Self> {
        let res = match data_type {
            DataType::Boolean => Self::Bool(BooleanBuilder::new(DEFAULT_CAPACITY)),
            DataType::Int8 => Self::I8(Int8Builder::new(DEFAULT_CAPACITY)),
            DataType::Int16 => Self::I16(Int16Builder::new(DEFAULT_CAPACITY)),
            DataType::Int32 => Self::I32(Int32Builder::new(DEFAULT_CAPACITY)),
            DataType::Int64 => Self::I64(Int64Builder::new(DEFAULT_CAPACITY)),
            DataType::UInt8 => Self::U8(UInt8Builder::new(DEFAULT_CAPACITY)),
            DataType::UInt16 => Self::U16(UInt16Builder::new(DEFAULT_CAPACITY)),
            DataType::UInt32 => Self::U32(UInt32Builder::new(DEFAULT_CAPACITY)),
            DataType::UInt64 => Self::U64(UInt64Builder::new(DEFAULT_CAPACITY)),
            DataType::Float32 => Self::F32(Float32Builder::new(DEFAULT_CAPACITY)),
            DataType::Float64 => Self::F64(Float64Builder::new(DEFAULT_CAPACITY)),
            DataType::Utf8 => Self::Utf8(StringBuilder::new(DEFAULT_CAPACITY)),
            DataType::LargeUtf8 => Self::LargeUtf8(LargeStringBuilder::new(DEFAULT_CAPACITY)),
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

macro_rules! simple_append {
    ($name:ident, $ty:ty, $variant:ident) => {
        pub fn $name(&mut self, value: $ty) -> Result<()> {
            match self {
                Self::$variant(builder) => builder.append_value(value)?,
                _ => fail!("Mismatched type: cannot insert {}", stringify!($ty)),
            };
            Ok(())
        }
    };
}

impl ArrayBuilder {
    simple_append!(append_bool, bool, Bool);
    simple_append!(append_i8, i8, I8);
    simple_append!(append_i16, i16, I16);
    simple_append!(append_i32, i32, I32);
    simple_append!(append_i64, i64, I64);
    simple_append!(append_u8, u8, U8);
    simple_append!(append_u16, u16, U16);
    simple_append!(append_u32, u32, U32);
    simple_append!(append_u64, u64, U64);
    simple_append!(append_f32, f32, F32);
    simple_append!(append_f64, f64, F64);

    pub fn append_utf8(&mut self, data: &str) -> Result<()> {
        match self {
            Self::Utf8(builder) => builder.append_value(data)?,
            Self::LargeUtf8(builder) => builder.append_value(data)?,
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

macro_rules! simple_serialize {
    ($name:ident, $ty:ty, $func:ident) => {
        fn $name(self, value: $ty) -> Result<Self::Ok> {
            self.$func(value)
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

    simple_serialize!(serialize_bool, bool, append_bool);
    simple_serialize!(serialize_i8, i8, append_i8);
    simple_serialize!(serialize_i16, i16, append_i16);
    simple_serialize!(serialize_i32, i32, append_i32);
    simple_serialize!(serialize_i64, i64, append_i64);
    simple_serialize!(serialize_u8, u8, append_u8);
    simple_serialize!(serialize_u16, u16, append_u16);
    simple_serialize!(serialize_u32, u32, append_u32);
    simple_serialize!(serialize_u64, u64, append_u64);
    simple_serialize!(serialize_f32, f32, append_f32);
    simple_serialize!(serialize_f64, f64, append_f64);

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
