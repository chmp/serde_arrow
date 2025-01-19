//! Serialize values into a in-memory representation
use serde::{de::DeserializeOwned, forward_to_deserialize_any, Serialize};

use crate::internal::error::{fail, Error, Result};

#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub struct Variant(u32, &'static str);

/// A in-memory representation of a Serde value
///
/// Values are comparable and hashable with a couple of caveats:
///
/// - Hash and equality for structs / maps are dependent on the field / entry
///   order
/// - Hash and equality of floats are based on the underlying bits, not the
///   resulting floating point values
///
#[derive(Debug, Clone, PartialEq, Eq, Hash)]
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
    F32(HashF32),
    F64(HashF64),
    StaticStr(&'static str),
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
    StructVariant(Variant, Vec<(&'static str, Value)>),
    TupleVariant(Variant, Vec<Value>),
    UnitVariant(Variant),
    NewtypeVariant(Variant, Box<Value>),
}

#[derive(Debug, Clone, Copy)]
pub struct HashF32(f32);

impl std::cmp::PartialEq<HashF32> for HashF32 {
    fn eq(&self, other: &Self) -> bool {
        self.0.to_ne_bytes() == other.0.to_ne_bytes()
    }
}

impl std::cmp::Eq for HashF32 {}

impl std::hash::Hash for HashF32 {
    fn hash<H: std::hash::Hasher>(&self, state: &mut H) {
        self.0.to_ne_bytes().hash(state)
    }
}

#[derive(Debug, Clone, Copy)]
pub struct HashF64(f64);

impl std::cmp::PartialEq<HashF64> for HashF64 {
    fn eq(&self, other: &Self) -> bool {
        self.0.to_ne_bytes() == other.0.to_ne_bytes()
    }
}

impl std::cmp::Eq for HashF64 {}

impl std::hash::Hash for HashF64 {
    fn hash<H: std::hash::Hasher>(&self, state: &mut H) {
        self.0.to_ne_bytes().hash(state)
    }
}

pub fn transmute<T: DeserializeOwned>(value: impl Serialize) -> Result<T> {
    let value = value.serialize(ValueSerializer)?;
    T::deserialize(ValueDeserializer::new(&value))
}

impl<'a> TryFrom<&'a Value> for &'a str {
    type Error = Error;

    fn try_from(value: &'a Value) -> Result<Self> {
        match value {
            Value::StaticStr(s) => Ok(s),
            Value::String(s) => Ok(s),
            _ => fail!("Cannot extract string from non-string value"),
        }
    }
}

macro_rules! impl_try_from_value_for_int {
    ($ty:ty) => {
        impl<'a> TryFrom<&'a Value> for $ty {
            type Error = Error;

            fn try_from(value: &'a Value) -> Result<Self, Self::Error> {
                match value {
                    &Value::U8(v) => Ok(v.try_into()?),
                    &Value::U16(v) => Ok(v.try_into()?),
                    &Value::U32(v) => Ok(v.try_into()?),
                    &Value::U64(v) => Ok(v.try_into()?),
                    &Value::I8(v) => Ok(v.try_into()?),
                    &Value::I16(v) => Ok(v.try_into()?),
                    &Value::I32(v) => Ok(v.try_into()?),
                    &Value::I64(v) => Ok(v.try_into()?),
                    _ => fail!("Cannot extract integer from non-integer value"),
                }
            }
        }
    };
}

impl_try_from_value_for_int!(i8);
impl_try_from_value_for_int!(i16);
impl_try_from_value_for_int!(i32);
impl_try_from_value_for_int!(i64);
impl_try_from_value_for_int!(u8);
impl_try_from_value_for_int!(u16);
impl_try_from_value_for_int!(u32);
impl_try_from_value_for_int!(u64);

pub struct ValueSerializer;

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
        Ok(Value::F32(HashF32(v)))
    }

    fn serialize_f64(self, v: f64) -> Result<Self::Ok> {
        Ok(Value::F64(HashF64(v)))
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
        _name: &'static str,
        variant_index: u32,
        variant: &'static str,
        len: usize,
    ) -> Result<Self::SerializeStructVariant> {
        Ok(StructVariantSerializer::new(variant_index, variant, len))
    }

    fn serialize_tuple_variant(
        self,
        _name: &'static str,
        variant_index: u32,
        variant: &'static str,
        len: usize,
    ) -> Result<Self::SerializeTupleVariant> {
        Ok(TupleVariantSerializer::new(variant_index, variant, len))
    }

    fn serialize_newtype_variant<T: Serialize + ?Sized>(
        self,
        _name: &'static str,
        variant_index: u32,
        variant: &'static str,
        value: &T,
    ) -> Result<Self::Ok> {
        let value = value.serialize(ValueSerializer)?;
        Ok(Value::NewtypeVariant(
            Variant(variant_index, variant),
            Box::new(value),
        ))
    }

    fn serialize_unit_variant(
        self,
        _name: &'static str,
        variant_index: u32,
        variant: &'static str,
    ) -> Result<Self::Ok> {
        Ok(Value::UnitVariant(Variant(variant_index, variant)))
    }
}

pub struct SeqSerializer(Vec<Value>);

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
pub struct TupleStructSerializer {
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

pub struct MapSerializer {
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

pub struct StructSerializer {
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

pub struct StructVariantSerializer {
    variant_index: u32,
    variant_name: &'static str,
    values: Vec<(&'static str, Value)>,
}

impl StructVariantSerializer {
    fn new(variant_index: u32, variant_name: &'static str, len: usize) -> Self {
        Self {
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
            Variant(self.variant_index, self.variant_name),
            self.values,
        ))
    }
}

pub struct TupleVariantSerializer {
    variant_index: u32,
    variant_name: &'static str,
    values: Vec<Value>,
}

impl TupleVariantSerializer {
    fn new(variant_index: u32, variant_name: &'static str, len: usize) -> Self {
        Self {
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
            Variant(self.variant_index, self.variant_name),
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

pub struct ValueDeserializer<'a>(&'a Value);

impl<'a> ValueDeserializer<'a> {
    pub fn new(value: &'a Value) -> Self {
        Self(value)
    }
}

impl<'de> serde::de::Deserializer<'de> for ValueDeserializer<'_> {
    type Error = Error;

    fn deserialize_any<V: serde::de::Visitor<'de>>(self, visitor: V) -> Result<V::Value> {
        match self.0 {
            Value::Unit => visitor.visit_unit(),
            &Value::Char(v) => visitor.visit_char(v),
            &Value::Bool(v) => visitor.visit_bool(v),
            &Value::U8(v) => visitor.visit_u8(v),
            &Value::U16(v) => visitor.visit_u16(v),
            &Value::U32(v) => visitor.visit_u32(v),
            &Value::U64(v) => visitor.visit_u64(v),
            &Value::I8(v) => visitor.visit_i8(v),
            &Value::I16(v) => visitor.visit_i16(v),
            &Value::I32(v) => visitor.visit_i32(v),
            &Value::I64(v) => visitor.visit_i64(v),
            &Value::F32(v) => visitor.visit_f32(v.0),
            &Value::F64(v) => visitor.visit_f64(v.0),
            Value::String(v) => visitor.visit_str(v),
            &Value::StaticStr(v) => visitor.visit_str(v),
            Value::Bytes(v) => visitor.visit_bytes(v),
            Value::Seq(v) => visitor.visit_seq(SeqDeserializer::new(v)),
            Value::Tuple(v) => visitor.visit_seq(SeqDeserializer::new(v)),
            Value::Struct(_, entries) => visitor.visit_map(StructDeserializer::new(entries)),
            Value::Map(entries) => visitor.visit_map(MapDeserializer::new(entries)),
            Value::NewtypeStruct(_, value) => {
                ValueDeserializer::new(value).deserialize_any(visitor)
            }
            &Value::UnitVariant(variant) => visitor.visit_enum(UnitVariantDeserializer(variant)),
            Value::TupleVariant(variant, values) => {
                visitor.visit_enum(TupleVariantDeserializer(*variant, values))
            }
            Value::NewtypeVariant(variant, value) => {
                visitor.visit_enum(NewTypeVariantDeserializer(*variant, value))
            }
            Value::StructVariant(variant, fields) => {
                visitor.visit_enum(StructVariantDeserializer(*variant, fields))
            }
            Value::Some(v) => visitor.visit_some(ValueDeserializer::new(v)),
            Value::None => visitor.visit_none(),
            Value::UnitStruct(_) => visitor.visit_unit(),
            Value::TupleStruct(_, values) => visitor.visit_seq(SeqDeserializer::new(values)),
        }
    }

    fn deserialize_ignored_any<V: serde::de::Visitor<'de>>(self, visitor: V) -> Result<V::Value> {
        self.deserialize_any(visitor)
    }

    fn deserialize_byte_buf<V: serde::de::Visitor<'de>>(self, visitor: V) -> Result<V::Value> {
        match self.0 {
            Value::Bytes(v) => visitor.visit_byte_buf(v.to_owned()),
            Value::String(v) => visitor.visit_byte_buf(v.as_bytes().to_owned()),
            v => fail!("Cannot deserialize bytes from non-bytes value {v:?}"),
        }
    }

    fn deserialize_bytes<V: serde::de::Visitor<'de>>(self, visitor: V) -> Result<V::Value> {
        match self.0 {
            Value::Bytes(v) => visitor.visit_bytes(v),
            Value::String(v) => visitor.visit_bytes(v.as_bytes()),
            v => fail!("Cannot deserialize bytes from non-bytes value {v:?}"),
        }
    }

    fn deserialize_char<V: serde::de::Visitor<'de>>(self, visitor: V) -> Result<V::Value> {
        match self.0 {
            &Value::Char(v) => visitor.visit_char(v),
            v => fail!("Cannot deserializer char from non-char value {v:?}"),
        }
    }

    fn deserialize_enum<V: serde::de::Visitor<'de>>(
        self,
        _name: &'static str,
        _variants: &'static [&'static str],
        visitor: V,
    ) -> Result<V::Value> {
        match self.0 {
            &Value::UnitVariant(variant) => visitor.visit_enum(UnitVariantDeserializer(variant)),
            Value::TupleVariant(variant, values) => {
                visitor.visit_enum(TupleVariantDeserializer(*variant, values))
            }
            Value::NewtypeVariant(variant, value) => {
                visitor.visit_enum(NewTypeVariantDeserializer(*variant, value))
            }
            Value::StructVariant(variant, fields) => {
                visitor.visit_enum(StructVariantDeserializer(*variant, fields))
            }
            v => fail!("Cannot deserialize enum from non-enum value {v:?}"),
        }
    }

    fn deserialize_bool<V: serde::de::Visitor<'de>>(self, visitor: V) -> Result<V::Value> {
        let &Value::Bool(v) = self.0 else {
            fail!("Cannot deserialize bool from non-bool {:?}", self.0);
        };
        visitor.visit_bool(v)
    }

    fn deserialize_u8<V: serde::de::Visitor<'de>>(self, visitor: V) -> Result<V::Value> {
        visitor.visit_u8(self.0.try_into()?)
    }

    fn deserialize_u16<V: serde::de::Visitor<'de>>(self, visitor: V) -> Result<V::Value> {
        visitor.visit_u16(self.0.try_into()?)
    }

    fn deserialize_u32<V: serde::de::Visitor<'de>>(self, visitor: V) -> Result<V::Value> {
        visitor.visit_u32(self.0.try_into()?)
    }

    fn deserialize_u64<V: serde::de::Visitor<'de>>(self, visitor: V) -> Result<V::Value> {
        visitor.visit_u64(self.0.try_into()?)
    }

    fn deserialize_i8<V: serde::de::Visitor<'de>>(self, visitor: V) -> Result<V::Value> {
        visitor.visit_i8(self.0.try_into()?)
    }

    fn deserialize_i16<V: serde::de::Visitor<'de>>(self, visitor: V) -> Result<V::Value> {
        visitor.visit_i16(self.0.try_into()?)
    }

    fn deserialize_i32<V: serde::de::Visitor<'de>>(self, visitor: V) -> Result<V::Value> {
        visitor.visit_i32(self.0.try_into()?)
    }

    fn deserialize_i64<V: serde::de::Visitor<'de>>(self, visitor: V) -> Result<V::Value> {
        visitor.visit_i64(self.0.try_into()?)
    }

    fn deserialize_f32<V: serde::de::Visitor<'de>>(self, visitor: V) -> Result<V::Value> {
        match self.0 {
            &Value::F32(v) => visitor.visit_f32(v.0),
            &Value::F64(v) => visitor.visit_f32(v.0 as f32),
            v => fail!("Cannot deserialize f32 from non-float value {v:?}"),
        }
    }

    fn deserialize_f64<V: serde::de::Visitor<'de>>(self, visitor: V) -> Result<V::Value> {
        match self.0 {
            &Value::F32(v) => visitor.visit_f64(v.0 as f64),
            &Value::F64(v) => visitor.visit_f64(v.0),
            v => fail!("Cannot deserialize f64 from non-float value {v:?}"),
        }
    }

    fn deserialize_identifier<V: serde::de::Visitor<'de>>(self, visitor: V) -> Result<V::Value> {
        visitor.visit_str(self.0.try_into()?)
    }

    fn deserialize_map<V: serde::de::Visitor<'de>>(self, visitor: V) -> Result<V::Value> {
        match self.0 {
            Value::Map(entries) => visitor.visit_map(MapDeserializer::new(entries)),
            Value::Struct(_, fields) => visitor.visit_map(StructDeserializer::new(fields)),
            v => fail!("Cannot deserialize a map from a non-map value {:?}", v),
        }
    }

    fn deserialize_newtype_struct<V: serde::de::Visitor<'de>>(
        self,
        _name: &'static str,
        visitor: V,
    ) -> Result<V::Value> {
        match self.0 {
            Value::NewtypeStruct(_, value) => {
                visitor.visit_newtype_struct(ValueDeserializer::new(value))
            }
            value => visitor.visit_newtype_struct(ValueDeserializer::new(value)),
        }
    }

    fn deserialize_option<V: serde::de::Visitor<'de>>(self, visitor: V) -> Result<V::Value> {
        match self.0 {
            Value::Unit => visitor.visit_none(),
            Value::None => visitor.visit_none(),
            Value::Some(value) => visitor.visit_some(ValueDeserializer::new(value)),
            value => visitor.visit_some(ValueDeserializer::new(value)),
        }
    }

    fn deserialize_seq<V: serde::de::Visitor<'de>>(self, visitor: V) -> Result<V::Value> {
        match self.0 {
            Value::Seq(values) => visitor.visit_seq(SeqDeserializer::new(values)),
            Value::Tuple(values) => visitor.visit_seq(SeqDeserializer::new(values)),
            v => fail!("Cannot deserialize sequence from non-sequence value {v:?}"),
        }
    }

    fn deserialize_str<V: serde::de::Visitor<'de>>(self, visitor: V) -> Result<V::Value> {
        visitor.visit_str(self.0.try_into()?)
    }

    fn deserialize_string<V: serde::de::Visitor<'de>>(self, visitor: V) -> Result<V::Value> {
        visitor.visit_str(self.0.try_into()?)
    }

    fn deserialize_struct<V: serde::de::Visitor<'de>>(
        self,
        _name: &'static str,
        _fields: &'static [&'static str],
        visitor: V,
    ) -> Result<V::Value> {
        match self.0 {
            Value::Struct(_name, fields) => visitor.visit_map(StructDeserializer::new(fields)),
            Value::Map(entries) => visitor.visit_map(MapDeserializer::new(entries)),
            v => fail!("Cannot deserialize struct from non-struct value {v:?}"),
        }
    }

    fn deserialize_tuple<V: serde::de::Visitor<'de>>(
        self,
        _len: usize,
        visitor: V,
    ) -> Result<V::Value> {
        match self.0 {
            Value::Seq(values) => visitor.visit_seq(SeqDeserializer::new(values)),
            Value::Tuple(values) => visitor.visit_seq(SeqDeserializer::new(values)),
            v => fail!("Cannot deserialize tuple from non-sequence value {v:?}"),
        }
    }

    fn deserialize_tuple_struct<V: serde::de::Visitor<'de>>(
        self,
        _name: &'static str,
        _len: usize,
        visitor: V,
    ) -> Result<V::Value> {
        match self.0 {
            Value::TupleStruct(_, values) => visitor.visit_seq(SeqDeserializer::new(values)),
            v => fail!("Cannot deserialize tuple struct from non-tuple-struct value {v:?}"),
        }
    }

    fn deserialize_unit<V: serde::de::Visitor<'de>>(self, visitor: V) -> Result<V::Value> {
        match self.0 {
            Value::Unit => visitor.visit_unit(),
            v => fail!("Cannot deserialize unit from non-unit value {v:?}"),
        }
    }

    fn deserialize_unit_struct<V: serde::de::Visitor<'de>>(
        self,
        _name: &'static str,
        visitor: V,
    ) -> Result<V::Value> {
        match self.0 {
            Value::UnitStruct(_) => visitor.visit_unit(),
            v => fail!("Cannot deserialize unit from non-unit value {v:?}"),
        }
    }

    fn is_human_readable(&self) -> bool {
        false
    }
}

struct StructDeserializer<'a>(&'a [(&'static str, Value)], Option<&'a Value>);

impl<'a> StructDeserializer<'a> {
    fn new(entries: &'a [(&'static str, Value)]) -> Self {
        Self(entries, None)
    }
}

impl<'de> serde::de::MapAccess<'de> for StructDeserializer<'_> {
    type Error = Error;

    fn next_key_seed<K: serde::de::DeserializeSeed<'de>>(
        &mut self,
        seed: K,
    ) -> Result<Option<K::Value>> {
        let [(key, value), next_fields @ ..] = self.0 else {
            return Ok(None);
        };
        let key = Value::StaticStr(key);
        let key = seed.deserialize(ValueDeserializer::new(&key))?;

        *self = Self(next_fields, Some(value));

        Ok(Some(key))
    }

    fn next_value_seed<V: serde::de::DeserializeSeed<'de>>(&mut self, seed: V) -> Result<V::Value> {
        let Some(value) = self.1.take() else {
            fail!("Invalid usage");
        };
        seed.deserialize(ValueDeserializer::new(value))
    }
}

pub struct SeqDeserializer<'a>(&'a [Value]);

impl<'a> SeqDeserializer<'a> {
    pub fn new(values: &'a [Value]) -> Self {
        Self(values)
    }
}

impl<'de> serde::de::SeqAccess<'de> for SeqDeserializer<'_> {
    type Error = Error;

    fn next_element_seed<T: serde::de::DeserializeSeed<'de>>(
        &mut self,
        seed: T,
    ) -> Result<Option<T::Value>> {
        let [item, new_values @ ..] = self.0 else {
            return Ok(None);
        };
        let item = seed.deserialize(ValueDeserializer(item))?;
        self.0 = new_values;
        Ok(Some(item))
    }
}

pub struct MapDeserializer<'a>(&'a [(Value, Value)], Option<&'a Value>);

impl<'a> MapDeserializer<'a> {
    pub fn new(entries: &'a [(Value, Value)]) -> Self {
        Self(entries, None)
    }
}

impl<'de> serde::de::MapAccess<'de> for MapDeserializer<'_> {
    type Error = Error;

    fn next_key_seed<K: serde::de::DeserializeSeed<'de>>(
        &mut self,
        seed: K,
    ) -> Result<Option<K::Value>> {
        let [(key, value), next_entries @ ..] = self.0 else {
            return Ok(None);
        };
        let key = seed.deserialize(ValueDeserializer::new(key))?;

        *self = Self(next_entries, Some(value));
        Ok(Some(key))
    }

    fn next_value_seed<V: serde::de::DeserializeSeed<'de>>(&mut self, seed: V) -> Result<V::Value> {
        let Some(value) = self.1.take() else {
            fail!("Invalid usage");
        };
        seed.deserialize(ValueDeserializer::new(value))
    }
}

struct UnitVariantDeserializer(Variant);

impl<'de> serde::de::EnumAccess<'de> for UnitVariantDeserializer {
    type Error = Error;
    type Variant = UnitVariantVariant;

    fn variant_seed<V: serde::de::DeserializeSeed<'de>>(
        self,
        seed: V,
    ) -> Result<(V::Value, Self::Variant)> {
        let variant = seed.deserialize(VariantDeserializer(self.0))?;
        Ok((variant, UnitVariantVariant))
    }
}

struct UnitVariantVariant;

impl<'de> serde::de::VariantAccess<'de> for UnitVariantVariant {
    type Error = Error;

    fn newtype_variant_seed<T: serde::de::DeserializeSeed<'de>>(
        self,
        _seed: T,
    ) -> Result<T::Value> {
        fail!("Invalid variant: expected unit variant found newtype variant")
    }

    fn struct_variant<V: serde::de::Visitor<'de>>(
        self,
        _fields: &'static [&'static str],
        _visitor: V,
    ) -> Result<V::Value> {
        fail!("Invalid variant: expected unit variant found struct variant")
    }

    fn tuple_variant<V: serde::de::Visitor<'de>>(
        self,
        _len: usize,
        _visitor: V,
    ) -> Result<V::Value> {
        fail!("Invalid variant: expected unit variant, found tuple variant")
    }

    fn unit_variant(self) -> Result<()> {
        Ok(())
    }
}

struct TupleVariantDeserializer<'a>(Variant, &'a [Value]);

impl<'de, 'a> serde::de::EnumAccess<'de> for TupleVariantDeserializer<'a> {
    type Error = Error;
    type Variant = TupleVariantVariant<'a>;

    fn variant_seed<V: serde::de::DeserializeSeed<'de>>(
        self,
        seed: V,
    ) -> Result<(V::Value, Self::Variant)> {
        let variant = seed.deserialize(VariantDeserializer(self.0))?;
        Ok((variant, TupleVariantVariant(self.1)))
    }
}

struct TupleVariantVariant<'a>(&'a [Value]);

impl<'de> serde::de::VariantAccess<'de> for TupleVariantVariant<'_> {
    type Error = Error;

    fn newtype_variant_seed<T: serde::de::DeserializeSeed<'de>>(
        self,
        _seed: T,
    ) -> Result<T::Value> {
        fail!("Invalid variant: expected tuple variant, found newtype variant")
    }

    fn unit_variant(self) -> std::prelude::v1::Result<(), Self::Error> {
        fail!("Invalid variant: expected tuple variant, found unit variant")
    }

    fn struct_variant<V: serde::de::Visitor<'de>>(
        self,
        _fields: &'static [&'static str],
        _visitor: V,
    ) -> Result<V::Value> {
        fail!("Invalid variant: expected tuple variant, found struct variant")
    }

    fn tuple_variant<V: serde::de::Visitor<'de>>(
        self,
        _len: usize,
        visitor: V,
    ) -> Result<V::Value> {
        visitor.visit_seq(SeqDeserializer::new(self.0))
    }
}

struct NewTypeVariantDeserializer<'a>(Variant, &'a Value);

impl<'de, 'a> serde::de::EnumAccess<'de> for NewTypeVariantDeserializer<'a> {
    type Error = Error;
    type Variant = NewTypeVariantVariant<'a>;

    fn variant_seed<V: serde::de::DeserializeSeed<'de>>(
        self,
        seed: V,
    ) -> Result<(V::Value, Self::Variant)> {
        let variant = seed.deserialize(VariantDeserializer(self.0))?;
        Ok((variant, NewTypeVariantVariant(self.1)))
    }
}

struct NewTypeVariantVariant<'a>(&'a Value);

impl<'de> serde::de::VariantAccess<'de> for NewTypeVariantVariant<'_> {
    type Error = Error;

    fn newtype_variant_seed<T: serde::de::DeserializeSeed<'de>>(self, seed: T) -> Result<T::Value> {
        seed.deserialize(ValueDeserializer::new(self.0))
    }

    fn unit_variant(self) -> std::prelude::v1::Result<(), Self::Error> {
        fail!("Invalid variant: expected newtype variant, found unit variant")
    }

    fn struct_variant<V: serde::de::Visitor<'de>>(
        self,
        _fields: &'static [&'static str],
        _visitor: V,
    ) -> Result<V::Value> {
        fail!("Invalid variant: expected newtype variant, found struct variant")
    }

    fn tuple_variant<V: serde::de::Visitor<'de>>(
        self,
        _len: usize,
        _visitor: V,
    ) -> Result<V::Value> {
        fail!("Invalid variant: expected newtype variant, found tuple variant")
    }
}

struct StructVariantDeserializer<'a>(Variant, &'a [(&'static str, Value)]);

impl<'de, 'a> serde::de::EnumAccess<'de> for StructVariantDeserializer<'a> {
    type Error = Error;
    type Variant = StructVariantVariant<'a>;

    fn variant_seed<V: serde::de::DeserializeSeed<'de>>(
        self,
        seed: V,
    ) -> Result<(V::Value, Self::Variant)> {
        let variant = seed.deserialize(VariantDeserializer(self.0))?;
        Ok((variant, StructVariantVariant(self.1)))
    }
}

struct StructVariantVariant<'a>(&'a [(&'static str, Value)]);

impl<'de> serde::de::VariantAccess<'de> for StructVariantVariant<'_> {
    type Error = Error;

    fn newtype_variant_seed<T: serde::de::DeserializeSeed<'de>>(
        self,
        _seed: T,
    ) -> Result<T::Value> {
        fail!("Invalid variant: expected struct variant, found newtype variant")
    }

    fn struct_variant<V: serde::de::Visitor<'de>>(
        self,
        _fields: &'static [&'static str],
        visitor: V,
    ) -> Result<V::Value> {
        visitor.visit_map(StructDeserializer::new(self.0))
    }

    fn tuple_variant<V: serde::de::Visitor<'de>>(
        self,
        _len: usize,
        _visitor: V,
    ) -> Result<V::Value> {
        fail!("Invalid variant: expected struct variant, found tuple variant")
    }

    fn unit_variant(self) -> Result<()> {
        fail!("Invalid variant: expected struct variant, found unit variant")
    }
}

struct VariantDeserializer(Variant);

impl<'de> serde::Deserializer<'de> for VariantDeserializer {
    type Error = Error;

    fn deserialize_any<V: serde::de::Visitor<'de>>(self, visitor: V) -> Result<V::Value> {
        self.deserialize_str(visitor)
    }

    fn deserialize_str<V: serde::de::Visitor<'de>>(self, visitor: V) -> Result<V::Value> {
        visitor.visit_str(self.0 .1)
    }

    fn deserialize_string<V: serde::de::Visitor<'de>>(self, visitor: V) -> Result<V::Value> {
        visitor.visit_str(self.0 .1)
    }

    fn deserialize_i8<V: serde::de::Visitor<'de>>(self, visitor: V) -> Result<V::Value> {
        visitor.visit_i8(self.0 .0.try_into()?)
    }

    fn deserialize_i16<V: serde::de::Visitor<'de>>(self, visitor: V) -> Result<V::Value> {
        visitor.visit_i16(self.0 .0.try_into()?)
    }

    fn deserialize_i32<V: serde::de::Visitor<'de>>(self, visitor: V) -> Result<V::Value> {
        visitor.visit_i32(self.0 .0.try_into()?)
    }

    fn deserialize_i64<V: serde::de::Visitor<'de>>(self, visitor: V) -> Result<V::Value> {
        visitor.visit_i64(self.0 .0.into())
    }

    fn deserialize_u8<V: serde::de::Visitor<'de>>(self, visitor: V) -> Result<V::Value> {
        visitor.visit_u8(self.0 .0.try_into()?)
    }

    fn deserialize_u16<V: serde::de::Visitor<'de>>(self, visitor: V) -> Result<V::Value> {
        visitor.visit_u16(self.0 .0.try_into()?)
    }

    fn deserialize_u32<V: serde::de::Visitor<'de>>(self, visitor: V) -> Result<V::Value> {
        visitor.visit_u32(self.0 .0)
    }

    fn deserialize_u64<V: serde::de::Visitor<'de>>(self, visitor: V) -> Result<V::Value> {
        visitor.visit_u64(self.0 .0.into())
    }

    forward_to_deserialize_any! {
        bool i128 u128 f32 f64 char bytes byte_buf option unit unit_struct
        newtype_struct seq tuple tuple_struct map struct enum identifier
        ignored_any
    }
}
