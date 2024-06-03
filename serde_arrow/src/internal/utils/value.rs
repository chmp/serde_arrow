//! Serialize values into a in-memory representation
use serde::Serialize;

use crate::{internal::error::fail, Error, Result};

#[derive(Debug)]
pub enum Value {
    Bool(bool),
    U8(u8),
    U16(u16),
    U32(u32),
    U64(u64),
    I8(i8),
    I16(i16),
    I32(i32),
    I64(i64),
    F32(f32),
    F64(f64),
    String(String),
    Char(char),
    Bytes(Vec<u8>),
    None,
    Some(Box<Value>),
    Unit,
    Tuple(Vec<Value>),
    Seq(Vec<Value>),
    Map(Vec<(Value, Value)>),
    Struct(&'static str, Vec<(&'static str, Value)>),
    NewtypeStruct(&'static str, Box<Value>),
    UnitStruct(&'static str),
    TupleStruct(&'static str, Vec<Value>),
    StructVariant(&'static str, u32, &'static str, Vec<(&'static str, Value)>),
    TupleVariant(&'static str, u32, &'static str, Vec<Value>),
    UnitVariant(&'static str, u32, &'static str),
    NewtypeVariant(&'static str, u32, &'static str, Box<Value>),
}

struct ValueSerializer;

impl serde::ser::Serializer for ValueSerializer {
    type Ok = Value;
    type Error = Error;

    type SerializeMap = MapSerializer;
    type SerializeSeq = SeqSerializer;
    type SerializeTuple = SeqSerializer;
    type SerializeStruct = StructSerializer;
    type SerializeTupleStruct = TupleStructSerializer;
    type SerializeStructVariant = StructVariantSerializer;
    type SerializeTupleVariant = TupleVariantSerializer;

    fn serialize_bool(self, v: bool) -> Result<Self::Ok> {
        Ok(Value::Bool(v))
    }

    fn serialize_u8(self, v: u8) -> Result<Self::Ok> {
        Ok(Value::U8(v))
    }

    fn serialize_u16(self, v: u16) -> Result<Self::Ok> {
        Ok(Value::U16(v))
    }

    fn serialize_u32(self, v: u32) -> Result<Self::Ok> {
        Ok(Value::U32(v))
    }

    fn serialize_u64(self, v: u64) -> Result<Self::Ok> {
        Ok(Value::U64(v))
    }

    fn serialize_i8(self, v: i8) -> Result<Self::Ok> {
        Ok(Value::I8(v))
    }

    fn serialize_i16(self, v: i16) -> Result<Self::Ok> {
        Ok(Value::I16(v))
    }

    fn serialize_i32(self, v: i32) -> Result<Self::Ok> {
        Ok(Value::I32(v))
    }

    fn serialize_i64(self, v: i64) -> Result<Self::Ok> {
        Ok(Value::I64(v))
    }

    fn serialize_f32(self, v: f32) -> Result<Self::Ok> {
        Ok(Value::F32(v))
    }

    fn serialize_f64(self, v: f64) -> Result<Self::Ok> {
        Ok(Value::F64(v))
    }

    fn serialize_str(self, v: &str) -> Result<Self::Ok> {
        Ok(Value::String(v.to_owned()))
    }

    fn serialize_char(self, v: char) -> Result<Self::Ok> {
        Ok(Value::Char(v))
    }

    fn serialize_bytes(self, v: &[u8]) -> Result<Self::Ok> {
        Ok(Value::Bytes(v.to_owned()))
    }

    fn serialize_none(self) -> Result<Self::Ok> {
        Ok(Value::None)
    }

    fn serialize_some<T: Serialize + ?Sized>(self, value: &T) -> Result<Self::Ok> {
        let v = value.serialize(ValueSerializer)?;
        Ok(Value::Some(Box::new(v)))
    }

    fn serialize_unit(self) -> Result<Self::Ok> {
        Ok(Value::Unit)
    }

    fn serialize_seq(self, len: Option<usize>) -> Result<Self::SerializeSeq> {
        Ok(SeqSerializer::new(len))
    }

    fn serialize_tuple(self, len: usize) -> Result<Self::SerializeTuple> {
        Ok(SeqSerializer::new(Some(len)))
    }

    fn serialize_struct(self, name: &'static str, _len: usize) -> Result<Self::SerializeStruct> {
        Ok(StructSerializer::new(name))
    }

    fn serialize_map(self, len: Option<usize>) -> Result<Self::SerializeMap> {
        Ok(MapSerializer::new(len))
    }

    fn serialize_newtype_struct<T: Serialize + ?Sized>(
        self,
        name: &'static str,
        value: &T,
    ) -> Result<Self::Ok> {
        let value = value.serialize(ValueSerializer)?;
        Ok(Value::NewtypeStruct(name, Box::new(value)))
    }

    fn serialize_unit_struct(self, name: &'static str) -> Result<Self::Ok> {
        Ok(Value::UnitStruct(name))
    }

    fn serialize_tuple_struct(
        self,
        name: &'static str,
        len: usize,
    ) -> Result<Self::SerializeTupleStruct> {
        Ok(TupleStructSerializer::new(name, Some(len)))
    }

    fn serialize_struct_variant(
        self,
        name: &'static str,
        variant_index: u32,
        variant: &'static str,
        len: usize,
    ) -> Result<Self::SerializeStructVariant> {
        Ok(StructVariantSerializer::new(
            name,
            variant_index,
            variant,
            len,
        ))
    }

    fn serialize_tuple_variant(
        self,
        name: &'static str,
        variant_index: u32,
        variant: &'static str,
        len: usize,
    ) -> Result<Self::SerializeTupleVariant> {
        Ok(TupleVariantSerializer::new(
            name,
            variant_index,
            variant,
            len,
        ))
    }

    fn serialize_newtype_variant<T: Serialize + ?Sized>(
        self,
        name: &'static str,
        variant_index: u32,
        variant: &'static str,
        value: &T,
    ) -> Result<Self::Ok> {
        let value = value.serialize(ValueSerializer)?;
        Ok(Value::NewtypeVariant(
            name,
            variant_index,
            variant,
            Box::new(value),
        ))
    }

    fn serialize_unit_variant(
        self,
        name: &'static str,
        variant_index: u32,
        variant: &'static str,
    ) -> Result<Self::Ok> {
        Ok(Value::UnitVariant(name, variant_index, variant))
    }
}

struct SeqSerializer(Vec<Value>);

impl SeqSerializer {
    pub fn new(capacity: Option<usize>) -> Self {
        Self(vec_with_optional_capacity(capacity))
    }
}

impl serde::ser::SerializeSeq for SeqSerializer {
    type Ok = Value;
    type Error = Error;

    fn serialize_element<T: Serialize + ?Sized>(&mut self, value: &T) -> Result<()> {
        self.0.push(value.serialize(ValueSerializer)?);
        Ok(())
    }

    fn end(self) -> Result<Self::Ok> {
        Ok(Value::Seq(self.0))
    }
}

impl serde::ser::SerializeTuple for SeqSerializer {
    type Ok = Value;
    type Error = Error;

    fn serialize_element<T: Serialize + ?Sized>(&mut self, value: &T) -> Result<()> {
        self.0.push(value.serialize(ValueSerializer)?);
        Ok(())
    }

    fn end(self) -> Result<Self::Ok> {
        Ok(Value::Tuple(self.0))
    }
}

#[derive(Debug)]
struct TupleStructSerializer {
    name: &'static str,
    values: Vec<Value>,
}

impl TupleStructSerializer {
    pub fn new(name: &'static str, capacity: Option<usize>) -> Self {
        Self {
            name,
            values: vec_with_optional_capacity(capacity),
        }
    }
}

impl serde::ser::SerializeTupleStruct for TupleStructSerializer {
    type Ok = Value;
    type Error = Error;

    fn serialize_field<T: Serialize + ?Sized>(&mut self, value: &T) -> Result<()> {
        self.values.push(value.serialize(ValueSerializer)?);
        Ok(())
    }

    fn end(self) -> Result<Self::Ok> {
        Ok(Value::TupleStruct(self.name, self.values))
    }
}

struct MapSerializer {
    next_key: Option<Value>,
    entries: Vec<(Value, Value)>,
}

impl MapSerializer {
    fn new(capacity: Option<usize>) -> Self {
        Self {
            entries: vec_with_optional_capacity(capacity),
            next_key: None,
        }
    }
}

impl serde::ser::SerializeMap for MapSerializer {
    type Ok = Value;
    type Error = Error;

    fn serialize_key<T: Serialize + ?Sized>(&mut self, key: &T) -> Result<()> {
        if self.next_key.is_some() {
            fail!(
                "Invalid call to serialize_key: serialize_key must be followed by serialize_value"
            );
        }
        self.next_key = Some(key.serialize(ValueSerializer)?);
        Ok(())
    }

    fn serialize_value<T: Serialize + ?Sized>(&mut self, value: &T) -> Result<()> {
        let Some(key) = self.next_key.take() else {
            fail!("Invalid call to serialize_value: serialize_value must be preceded by serialize_key");
        };
        let value = value.serialize(ValueSerializer)?;
        self.entries.push((key, value));
        Ok(())
    }

    fn end(self) -> Result<Self::Ok> {
        if self.next_key.is_some() {
            fail!("Invalid call to end: serialize_key must be followed by serialize_value before calling end");
        }
        Ok(Value::Map(self.entries))
    }
}

struct StructSerializer {
    name: &'static str,
    entries: Vec<(&'static str, Value)>,
}

impl StructSerializer {
    pub fn new(name: &'static str) -> Self {
        Self {
            name,
            entries: Vec::new(),
        }
    }
}

impl serde::ser::SerializeStruct for StructSerializer {
    type Ok = Value;
    type Error = Error;

    fn serialize_field<T: Serialize + ?Sized>(
        &mut self,
        key: &'static str,
        value: &T,
    ) -> Result<()> {
        let value = value.serialize(ValueSerializer)?;
        self.entries.push((key, value));
        Ok(())
    }

    fn end(self) -> Result<Self::Ok> {
        Ok(Value::Struct(self.name, self.entries))
    }
}

struct StructVariantSerializer {
    name: &'static str,
    variant_index: u32,
    variant_name: &'static str,
    values: Vec<(&'static str, Value)>,
}

impl StructVariantSerializer {
    fn new(name: &'static str, variant_index: u32, variant_name: &'static str, len: usize) -> Self {
        Self {
            name,
            variant_index,
            variant_name,
            values: Vec::with_capacity(len),
        }
    }
}

impl serde::ser::SerializeStructVariant for StructVariantSerializer {
    type Ok = Value;
    type Error = Error;

    fn serialize_field<T: Serialize + ?Sized>(
        &mut self,
        key: &'static str,
        value: &T,
    ) -> Result<()> {
        let value = value.serialize(ValueSerializer)?;
        self.values.push((key, value));
        Ok(())
    }

    fn end(self) -> Result<Self::Ok> {
        Ok(Value::StructVariant(
            self.name,
            self.variant_index,
            self.variant_name,
            self.values,
        ))
    }
}

struct TupleVariantSerializer {
    name: &'static str,
    variant_index: u32,
    variant_name: &'static str,
    values: Vec<Value>,
}

impl TupleVariantSerializer {
    fn new(name: &'static str, variant_index: u32, variant_name: &'static str, len: usize) -> Self {
        Self {
            name,
            variant_index,
            variant_name,
            values: Vec::with_capacity(len),
        }
    }
}

impl serde::ser::SerializeTupleVariant for TupleVariantSerializer {
    type Ok = Value;
    type Error = Error;

    fn serialize_field<T: Serialize + ?Sized>(&mut self, value: &T) -> Result<()> {
        let value = value.serialize(ValueSerializer)?;
        self.values.push(value);
        Ok(())
    }

    fn end(self) -> Result<Self::Ok> {
        Ok(Value::TupleVariant(
            self.name,
            self.variant_index,
            self.variant_name,
            self.values,
        ))
    }
}

fn vec_with_optional_capacity<T>(capacity: Option<usize>) -> Vec<T> {
    if let Some(capacity) = capacity {
        Vec::with_capacity(capacity)
    } else {
        Vec::new()
    }
}
