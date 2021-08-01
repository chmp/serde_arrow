use serde::ser::{Impossible, Serialize, Serializer};

use crate::{fail, Error, Result};

macro_rules! unsupported {
    ($name:ident) => {
        fn $name(self) -> Result<Self::Ok> {
            return Err(Error::Custom(format!(
                "{} not supported for in schema tracing",
                stringify!($name)
            )));
        }
    };
    ($name:ident, $($ty:ty),*) => {
        fn $name(self, $(_: $ty),*) -> Result<Self::Ok> {
            return Err(Error::Custom(format!(
                "{} not supported for in schema tracing",
                stringify!($name)
            )));
        }
    };
}

/// Extract a string from a serializable object
pub struct StringExtractor;

impl Serializer for StringExtractor {
    type Ok = String;
    type Error = Error;

    type SerializeSeq = Impossible<Self::Ok, Self::Error>;
    type SerializeStruct = Impossible<Self::Ok, Self::Error>;
    type SerializeMap = Impossible<Self::Ok, Self::Error>;
    type SerializeTuple = Impossible<Self::Ok, Self::Error>;
    type SerializeTupleStruct = Impossible<Self::Ok, Self::Error>;
    type SerializeTupleVariant = Impossible<Self::Ok, Self::Error>;
    type SerializeStructVariant = Impossible<Self::Ok, Self::Error>;

    unsupported!(serialize_bool, bool);
    unsupported!(serialize_i8, i8);
    unsupported!(serialize_i16, i16);
    unsupported!(serialize_i32, i32);
    unsupported!(serialize_i64, i64);
    unsupported!(serialize_u8, u8);
    unsupported!(serialize_u16, u16);
    unsupported!(serialize_u32, u32);
    unsupported!(serialize_u64, u64);
    unsupported!(serialize_f32, f32);
    unsupported!(serialize_f64, f64);
    unsupported!(serialize_char, char);

    fn serialize_str(self, v: &str) -> Result<Self::Ok, Self::Error> {
        Ok(v.to_owned())
    }

    unsupported!(serialize_bytes, &[u8]);
    unsupported!(serialize_none);

    fn serialize_some<T: ?Sized + Serialize>(self, _: &T) -> Result<Self::Ok> {
        fail!("serialize_unit  not supported in schema tracing");
    }

    fn serialize_unit(self) -> Result<Self::Ok> {
        fail!("serialize_unit  not supported in schema tracing");
    }

    fn serialize_unit_struct(self, _name: &'static str) -> Result<Self::Ok> {
        fail!("serialize_unit_struct not supported in schema tracing");
    }

    fn serialize_unit_variant(
        self,
        _name: &'static str,
        _variant_index: u32,
        _variant: &'static str,
    ) -> Result<Self::Ok> {
        fail!("serialize_unit_variant not supported in schema tracing");
    }

    fn serialize_newtype_struct<T>(self, _name: &'static str, _value: &T) -> Result<Self::Ok>
    where
        T: ?Sized + Serialize,
    {
        fail!("serialize_newtype_struct not supported in schema tracing");
    }

    fn serialize_newtype_variant<T>(
        self,
        _name: &'static str,
        _variant_index: u32,
        _variant: &'static str,
        _value: &T,
    ) -> Result<Self::Ok>
    where
        T: ?Sized + Serialize,
    {
        fail!("serialize_newtype_variant not supported in schema tracing");
    }

    fn serialize_seq(self, _len: Option<usize>) -> Result<Self::SerializeSeq> {
        fail!("serialize_seq not supported in schema tracing");
    }

    fn serialize_tuple(self, _len: usize) -> Result<Self::SerializeTuple> {
        fail!("serialize_tuple not supported in schema tracing");
    }

    fn serialize_tuple_struct(
        self,
        _name: &'static str,
        _len: usize,
    ) -> Result<Self::SerializeTupleStruct> {
        fail!("serialize_tuple_struct not supported in schema tracing");
    }

    fn serialize_tuple_variant(
        self,
        _name: &'static str,
        _variant_index: u32,
        _variant: &'static str,
        _len: usize,
    ) -> Result<Self::SerializeTupleVariant> {
        fail!("serialize_tuple_variant not supported in schema tracing");
    }

    fn serialize_map(self, _len: Option<usize>) -> Result<Self::SerializeMap> {
        fail!("serialize_map not supported in schema tracing");
    }

    fn serialize_struct(self, _name: &'static str, _len: usize) -> Result<Self::SerializeStruct> {
        fail!("serialize_struct not supported in schema tracing");
    }

    fn serialize_struct_variant(
        self,
        _name: &'static str,
        _variant_index: u32,
        _variant: &'static str,
        _len: usize,
    ) -> Result<Self::SerializeStructVariant> {
        fail!("serialize_struct_variant not supported in schema tracing");
    }
}
