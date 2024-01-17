use serde::Serialize;

use crate::{internal::error::fail, Error, Result};

/// A marker type to denote not supported operations
#[derive(Debug, Clone, Copy)]
pub struct NotImplemented;

#[allow(unused_variables)]
impl serde::Serializer for NotImplemented {
    type Error = Error;
    type Ok = ();

    type SerializeMap = NotImplemented;
    type SerializeSeq = NotImplemented;
    type SerializeStruct = NotImplemented;
    type SerializeTuple = NotImplemented;
    type SerializeStructVariant = NotImplemented;
    type SerializeTupleStruct = NotImplemented;
    type SerializeTupleVariant = NotImplemented;

    fn serialize_unit(self) -> Result<()> {
        fail!("Serializer::serialize_unit is not implemented")
    }

    fn serialize_none(self) -> Result<()> {
        fail!("Serializer::serialize_none is not implemented")
    }

    fn serialize_some<T: Serialize + ?Sized>(self, value: &T) -> Result<()> {
        fail!("Serializer::serialize_some is not implemented")
    }

    fn serialize_bool(self, v: bool) -> Result<()> {
        fail!("Serializer::serialize_bool is not implemented")
    }

    fn serialize_char(self, v: char) -> Result<()> {
        fail!("Serializer::serialize_char is not implemented")
    }

    fn serialize_u8(self, v: u8) -> Result<()> {
        fail!("Serializer::serialize_u8 is not implemented")
    }

    fn serialize_u16(self, v: u16) -> Result<()> {
        fail!("Serializer::serialize_u16 is not implemented")
    }

    fn serialize_u32(self, v: u32) -> Result<()> {
        fail!("Serializer::serialize_u32 is not implemented")
    }

    fn serialize_u64(self, v: u64) -> Result<()> {
        fail!("Serializer::serialize_u64 is not implemented")
    }

    fn serialize_i8(self, v: i8) -> Result<()> {
        fail!("Serializer::serialize_i8 is not implemented")
    }

    fn serialize_i16(self, v: i16) -> Result<()> {
        fail!("Serializer::serialize_i16 is not implemented")
    }

    fn serialize_i32(self, v: i32) -> Result<()> {
        fail!("Serializer::serialize_i32 is not implemented")
    }

    fn serialize_i64(self, v: i64) -> Result<()> {
        fail!("Serializer::serialize_i64 is not implemented")
    }

    fn serialize_f32(self, v: f32) -> Result<()> {
        fail!("Serializer::serialize_f32 is not implemented")
    }

    fn serialize_f64(self, v: f64) -> Result<()> {
        fail!("Serializer::serialize_f64 is not implemented")
    }

    fn serialize_bytes(self, v: &[u8]) -> Result<()> {
        fail!("Serializer::serialize_bytes is not implemented")
    }

    fn serialize_str(self, v: &str) -> Result<()> {
        fail!("Serializer::serialize_str is not implemented")
    }

    fn serialize_map(self, len: Option<usize>) -> Result<Self::SerializeMap> {
        Ok(NotImplemented)
    }

    fn serialize_seq(self, len: Option<usize>) -> Result<Self::SerializeSeq> {
        Ok(NotImplemented)
    }

    fn serialize_struct(self, name: &'static str, len: usize) -> Result<Self::SerializeStruct> {
        Ok(NotImplemented)
    }

    fn serialize_tuple(self, len: usize) -> Result<Self::SerializeTuple> {
        Ok(NotImplemented)
    }

    fn serialize_newtype_struct<T: Serialize + ?Sized>(
        self,
        name: &'static str,
        value: &T,
    ) -> Result<()> {
        fail!("Serializer::serialize_newtype_struct is not implemented")
    }

    fn serialize_newtype_variant<T: Serialize + ?Sized>(
        self,
        name: &'static str,
        variant_index: u32,
        variant: &'static str,
        value: &T,
    ) -> Result<()> {
        fail!("Serializer::serialize_newtype_variant is not implemented")
    }

    fn serialize_unit_struct(self, name: &'static str) -> Result<()> {
        fail!("Serializer::serialize_unit_struct is not implemented")
    }

    fn serialize_unit_variant(
        self,
        name: &'static str,
        variant_index: u32,
        variant: &'static str,
    ) -> Result<()> {
        fail!("Serializer::serialize_unit_variant is not implemented")
    }

    fn serialize_struct_variant(
        self,
        name: &'static str,
        variant_index: u32,
        variant: &'static str,
        len: usize,
    ) -> Result<Self::SerializeStructVariant> {
        Ok(NotImplemented)
    }

    fn serialize_tuple_struct(
        self,
        name: &'static str,
        len: usize,
    ) -> Result<Self::SerializeTupleStruct> {
        Ok(NotImplemented)
    }

    fn serialize_tuple_variant(
        self,
        name: &'static str,
        variant_index: u32,
        variant: &'static str,
        len: usize,
    ) -> Result<Self::SerializeTupleVariant> {
        Ok(NotImplemented)
    }
}

#[allow(unused_variables)]
impl serde::ser::SerializeMap for NotImplemented {
    type Ok = ();
    type Error = Error;

    fn serialize_key<T: serde::Serialize + ?Sized>(&mut self, key: &T) -> Result<()> {
        fail!("SerializeMap::serialize_key is not implemented");
    }

    fn serialize_value<T: serde::Serialize + ?Sized>(&mut self, value: &T) -> Result<()> {
        fail!("SerializeMap::serialize_value is not implemented")
    }

    fn end(self) -> Result<()> {
        fail!("SerializeMap::end is not implemented")
    }
}

#[allow(unused_variables)]
impl serde::ser::SerializeSeq for NotImplemented {
    type Ok = ();
    type Error = Error;

    fn serialize_element<T: Serialize + ?Sized>(&mut self, value: &T) -> Result<()> {
        fail!("SerializeSeq::serialize_element is not implemented");
    }

    fn end(self) -> Result<()> {
        fail!("SerializeSeq::end is not implemented");
    }
}

#[allow(unused_variables)]
impl serde::ser::SerializeStruct for NotImplemented {
    type Ok = ();
    type Error = Error;

    fn serialize_field<T: Serialize + ?Sized>(
        &mut self,
        key: &'static str,
        value: &T,
    ) -> Result<()> {
        fail!("SerializeStruct::serialize_field is not implemented");
    }

    fn end(self) -> Result<()> {
        fail!("SerializeStruct::end is not implemented");
    }
}

#[allow(unused_variables)]
impl serde::ser::SerializeTuple for NotImplemented {
    type Ok = ();
    type Error = Error;

    fn serialize_element<T: Serialize + ?Sized>(&mut self, value: &T) -> Result<()> {
        fail!("SerializeTuple::serialize_element is not implemented");
    }

    fn end(self) -> Result<()> {
        fail!("SerializeTuple::end is not implemented")
    }
}

#[allow(unused_variables)]
impl serde::ser::SerializeStructVariant for NotImplemented {
    type Ok = ();
    type Error = Error;

    fn serialize_field<T: Serialize + ?Sized>(
        &mut self,
        key: &'static str,
        value: &T,
    ) -> Result<()> {
        fail!("SerializeStructVariant::serialize_field is not implemented");
    }

    fn end(self) -> Result<()> {
        fail!("SerializeStructVariant::end is not implemented");
    }
}

#[allow(unused_variables)]
impl serde::ser::SerializeTupleStruct for NotImplemented {
    type Ok = ();
    type Error = Error;

    fn serialize_field<T: Serialize + ?Sized>(&mut self, value: &T) -> Result<()> {
        fail!("SerializeTupleStruct::serialize_field is not implemented");
    }

    fn end(self) -> Result<()> {
        fail!("SerializeTupleStruct::end is not implemented");
    }
}

#[allow(unused_variables)]
impl serde::ser::SerializeTupleVariant for NotImplemented {
    type Ok = ();
    type Error = Error;

    fn serialize_field<T: Serialize + ?Sized>(&mut self, value: &T) -> Result<()> {
        fail!("SerializeTupleVariant::serialize_field is not implemented");
    }

    fn end(self) -> Result<()> {
        fail!("SerializeTupleVariant::end is not implemented");
    }
}
