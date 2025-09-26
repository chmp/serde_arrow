//! An implementation using dynamic dispatch
use marrow::{
    array::{Array, BooleanArray, BytesArray, ListArray, PrimitiveArray, StructArray},
    datatypes::{DataType, Field, FieldMeta},
};
use serde::{ser::Impossible, Serialize};
use serde_arrow::{schema::SchemaLike, Error, Result};

use crate::mini_serde_arrow::utils::{unsupported, StaticFieldName};

pub fn trace(items: &(impl Serialize + ?Sized)) -> Vec<Field> {
    Vec::<Field>::from_samples(items, Default::default()).unwrap()
}

pub fn serialize(fields: &[Field], items: &(impl Serialize + ?Sized)) -> Vec<Array> {
    to_marrow(fields, items).unwrap()
}

pub fn to_marrow<T: ?Sized + Serialize>(fields: &[Field], items: &T) -> Result<Vec<Array>> {
    let mut serializers = Vec::with_capacity(fields.len());
    for field in fields {
        serializers.push(build_serializer(field)?);
    }
    let mut serializer = OuterSerializer::new(fields, serializers);
    items.serialize(&mut serializer)?;

    let mut result = Vec::new();
    for field in &mut serializer.serializers {
        result.push(field.build_array()?);
    }

    Ok(result)
}

struct OuterSerializer<'a> {
    fields: &'a [Field],
    field_names: Vec<Option<StaticFieldName>>,
    serializers: Vec<Box<dyn SimpleSerializer + 'a>>,
}

impl<'a> OuterSerializer<'a> {
    pub fn new(fields: &'a [Field], serializers: Vec<Box<dyn SimpleSerializer + 'a>>) -> Self {
        Self {
            fields,
            field_names: vec![None; fields.len()],
            serializers,
        }
    }
}

impl<'r, 'a> serde::ser::Serializer for &'r mut OuterSerializer<'a> {
    type Ok = ();
    type Error = Error;

    unsupported!(
        serialize_bool,
        serialize_bytes,
        serialize_char,
        serialize_f32,
        serialize_f64,
        serialize_i16,
        serialize_i32,
        serialize_i64,
        serialize_i8,
        serialize_map,
        serialize_newtype_struct,
        serialize_newtype_variant,
        serialize_none,
        serialize_some,
        serialize_str,
        serialize_struct,
        serialize_struct_variant,
        serialize_tuple,
        serialize_tuple_struct,
        serialize_tuple_variant,
        serialize_u16,
        serialize_u32,
        serialize_u64,
        serialize_u8,
        serialize_unit,
        serialize_unit_struct,
        serialize_unit_variant,
    );

    type SerializeSeq = Self;

    fn serialize_seq(
        self,
        len: Option<usize>,
    ) -> std::result::Result<Self::SerializeSeq, Self::Error> {
        if let Some(len) = len {
            for serializer in &mut self.serializers {
                SimpleSerializer::reserve(serializer.as_mut(), len);
            }
        }
        Ok(self)
    }
}

impl<'r, 'a> serde::ser::SerializeSeq for &'r mut OuterSerializer<'a> {
    type Ok = ();
    type Error = Error;

    fn serialize_element<T: Serialize + ?Sized>(
        &mut self,
        value: &T,
    ) -> std::result::Result<(), Self::Error> {
        value.serialize(OuterStructSerializer(self, 0))
    }

    fn end(self) -> std::result::Result<Self::Ok, Self::Error> {
        Ok(())
    }
}

struct OuterStructSerializer<'r, 'a>(&'r mut OuterSerializer<'a>, usize);

impl<'r, 'a> serde::ser::Serializer for OuterStructSerializer<'r, 'a> {
    type Ok = ();
    type Error = Error;

    unsupported!(
        serialize_bool,
        serialize_bytes,
        serialize_char,
        serialize_f32,
        serialize_f64,
        serialize_i16,
        serialize_i32,
        serialize_i64,
        serialize_i8,
        serialize_map,
        serialize_newtype_struct,
        serialize_newtype_variant,
        serialize_none,
        serialize_some,
        serialize_seq,
        serialize_str,
        serialize_struct_variant,
        serialize_tuple,
        serialize_tuple_struct,
        serialize_tuple_variant,
        serialize_u16,
        serialize_u32,
        serialize_u64,
        serialize_u8,
        serialize_unit,
        serialize_unit_struct,
        serialize_unit_variant,
    );

    type SerializeStruct = Self;

    fn serialize_struct(self, _: &'static str, _: usize) -> Result<Self> {
        Ok(self)
    }
}

impl<'r, 'a> serde::ser::SerializeStruct for OuterStructSerializer<'r, 'a> {
    type Ok = ();
    type Error = Error;

    fn serialize_field<T: Serialize + ?Sized>(
        &mut self,
        key: &'static str,
        value: &T,
    ) -> Result<()> {
        let current = self.1;
        if Some(StaticFieldName::new(key)) != self.0.field_names[current] {
            if self.0.fields[current].name != key {
                return Err(Error::custom(
                    "Out of order fields are not supported".into(),
                ));
            }
            self.0.field_names[current] = Some(StaticFieldName::new(key));
        }
        value.serialize(Mut(self.0.serializers[current].as_mut()))?;
        self.1 += 1;
        Ok(())
    }

    fn end(self) -> std::result::Result<Self::Ok, Self::Error> {
        if self.1 != self.0.fields.len() {
            return Err(Error::custom("Skipping fields is not supported".into()));
        }
        Ok(())
    }
}

type ArraySerializer<'a> = Box<dyn SimpleSerializer + 'a>;

fn build_serializer<'a>(field: &'a Field) -> Result<ArraySerializer<'a>> {
    match &field.data_type {
        DataType::Boolean => Ok(Box::new(BoolSerializer::new())),
        DataType::Float32 => Ok(Box::new(PrimitiveSerializer::<f32>::new())),
        DataType::Float64 => Ok(Box::new(PrimitiveSerializer::<f64>::new())),
        DataType::LargeUtf8 => Ok(Box::new(Utf8Serializer::new())),
        DataType::Struct(fields) => {
            let mut serializers = Vec::with_capacity(fields.len());
            for field in fields {
                serializers.push(build_serializer(field)?);
            }
            Ok(Box::new(StructSerializer::new(fields, serializers)))
        }
        DataType::LargeList(element) => {
            let serializer = build_serializer(element)?;
            Ok(Box::new(SeqSerializer::new(field, serializer)))
        }
        dt => Err(Error::custom(format!("Unkown data type {dt:?}"))),
    }
}

struct PrimitiveSerializer<T> {
    values: Vec<T>,
}

impl<T> PrimitiveSerializer<T> {
    pub fn new() -> Self {
        Self {
            values: Default::default(),
        }
    }
}

impl SimpleSerializer for PrimitiveSerializer<f32> {
    fn build_array(&mut self) -> Result<Array> {
        Ok(Array::Float32(PrimitiveArray {
            validity: None,
            values: std::mem::take(&mut self.values),
        }))
    }

    fn reserve(&mut self, additional: usize) {
        self.values.reserve(additional);
    }

    fn serialize_f32(&mut self, value: f32) -> Result<()> {
        self.values.push(value);
        Ok(())
    }
}

impl SimpleSerializer for PrimitiveSerializer<f64> {
    fn build_array(&mut self) -> Result<Array> {
        Ok(Array::Float64(PrimitiveArray {
            validity: None,
            values: std::mem::take(&mut self.values),
        }))
    }

    fn reserve(&mut self, additional: usize) {
        self.values.reserve(additional);
    }

    fn serialize_f64(&mut self, value: f64) -> Result<()> {
        self.values.push(value);
        Ok(())
    }
}

#[derive(Default)]
struct BoolSerializer {
    len: usize,
    values: Vec<u8>,
}

impl BoolSerializer {
    pub fn new() -> Self {
        Self::default()
    }
}

impl SimpleSerializer for BoolSerializer {
    fn build_array(&mut self) -> Result<Array> {
        Ok(Array::Boolean(BooleanArray {
            len: self.len,
            validity: None,
            values: std::mem::take(&mut self.values),
        }))
    }

    fn reserve(&mut self, additional: usize) {
        self.values.reserve(additional / 8);
    }

    fn serialize_bool(&mut self, value: bool) -> Result<()> {
        marrow::bits::push(&mut self.values, &mut self.len, value);
        Ok(())
    }
}

struct SeqSerializer<'a> {
    offsets: Vec<i64>,
    field: &'a Field,
    serializer: ArraySerializer<'a>,
}

impl<'a> SeqSerializer<'a> {
    pub fn new(field: &'a Field, serializer: ArraySerializer<'a>) -> Self {
        Self {
            offsets: vec![0],
            field,
            serializer,
        }
    }
}

impl<'a> SimpleSerializer for SeqSerializer<'a> {
    fn build_array(&mut self) -> Result<Array> {
        Ok(Array::LargeList(ListArray {
            validity: None,
            offsets: std::mem::replace(&mut self.offsets, vec![0]),
            elements: Box::new(self.serializer.build_array()?),
            meta: FieldMeta {
                name: self.field.name.clone(),
                ..Default::default()
            },
        }))
    }

    fn reserve(&mut self, additional: usize) {
        self.offsets.reserve(additional);
    }

    fn serialize_seq_start(&mut self, len: Option<usize>) -> Result<()> {
        if let Some(len) = len {
            self.serializer.reserve(len);
        }
        let Some(last) = self.offsets.last() else {
            return Err(Error::custom("invalid offset array".into()));
        };
        self.offsets.push(*last);
        Ok(())
    }

    fn serialize_seq_item(&mut self) -> Result<&mut dyn SimpleSerializer> {
        let Some(last) = self.offsets.last_mut() else {
            return Err(Error::custom("invalid offset array".into()));
        };
        *last += 1;
        Ok(self.serializer.as_mut())
    }

    fn serialize_seq_end(&mut self) -> Result<()> {
        Ok(())
    }
}

struct StructSerializer<'a> {
    fields: &'a [Field],
    field_names: Vec<Option<StaticFieldName>>,
    serializers: Vec<ArraySerializer<'a>>,
    next: usize,
    len: usize,
}

impl<'a> StructSerializer<'a> {
    pub fn new(fields: &'a [Field], serializers: Vec<ArraySerializer<'a>>) -> StructSerializer<'a> {
        Self {
            fields,
            field_names: vec![None; fields.len()],
            serializers,
            next: 0,
            len: 0,
        }
    }
}

impl<'a> SimpleSerializer for StructSerializer<'a> {
    fn build_array(&mut self) -> Result<Array> {
        let mut fields = Vec::new();
        for (meta, field) in std::iter::zip(self.fields, &mut self.serializers) {
            fields.push((
                FieldMeta {
                    name: meta.name.to_owned(),
                    ..Default::default()
                },
                field.build_array()?,
            ));
        }

        Ok(Array::Struct(StructArray {
            len: std::mem::take(&mut self.len),
            validity: None,
            fields,
        }))
    }

    fn reserve(&mut self, additional: usize) {
        for field in &mut self.serializers {
            field.reserve(additional);
        }
    }

    fn serialize_struct_start(&mut self, _name: &'static str, _len: usize) -> Result<()> {
        self.next = 0;
        self.len += 1;
        Ok(())
    }

    fn serialize_struct_field(&mut self, key: &'static str) -> Result<&mut dyn SimpleSerializer> {
        let current = self.next;
        if let Some(field_name) = self.field_names[current] {
            if field_name != StaticFieldName::new(key) {
                return Err(Error::custom("Out of order fields".into()));
            }
        } else {
            if self.fields[current].name != key {
                return Err(Error::custom("Out of order fields".into()));
            }
            self.field_names[current] = Some(StaticFieldName::new(key));
        }
        self.next += 1;
        Ok(self.serializers[current].as_mut())
    }

    fn serialize_struct_end(&mut self) -> Result<()> {
        if self.next != self.serializers.len() {
            return Err(Error::custom("Missing fields".into()));
        }
        Ok(())
    }
}

struct Utf8Serializer {
    offsets: Vec<i64>,
    data: Vec<u8>,
}

impl Utf8Serializer {
    pub fn new() -> Self {
        Self {
            offsets: vec![0],
            data: Vec::new(),
        }
    }
}

impl SimpleSerializer for Utf8Serializer {
    fn build_array(&mut self) -> Result<Array> {
        Ok(Array::LargeUtf8(BytesArray {
            validity: None,
            offsets: std::mem::replace(&mut self.offsets, vec![0]),
            data: std::mem::take(&mut self.data),
        }))
    }

    fn reserve(&mut self, additional: usize) {
        self.offsets.reserve(additional);
        self.data.reserve(additional * 8);
    }

    fn serialize_str(&mut self, value: &str) -> Result<()> {
        let Some(offset) = self.offsets.last() else {
            return Err(Error::custom("INvalid offset array".into()));
        };
        self.offsets.push(*offset + i64::try_from(value.len())?);
        self.data.extend(value.as_bytes());
        Ok(())
    }
}

trait SimpleSerializer {
    fn reserve(&mut self, _: usize) {}

    fn build_array(&mut self) -> Result<Array>;

    fn serialize_bool(&mut self, _: bool) -> Result<()> {
        Err(Error::custom("does not support bool".into()))
    }
    fn serialize_f32(&mut self, _: f32) -> Result<()> {
        Err(Error::custom("does not support f32".into()))
    }

    fn serialize_f64(&mut self, _: f64) -> Result<()> {
        Err(Error::custom("does not support f64".into()))
    }

    fn serialize_str(&mut self, _: &str) -> Result<()> {
        Err(Error::custom("does not support str".into()))
    }

    fn serialize_struct_start(&mut self, _name: &'static str, _len: usize) -> Result<()> {
        Err(Error::custom("does not support struct".into()))
    }

    fn serialize_struct_field(&mut self, _key: &'static str) -> Result<&mut dyn SimpleSerializer> {
        Err(Error::custom("does not support struct".into()))
    }

    fn serialize_struct_end(&mut self) -> Result<()> {
        Err(Error::custom("does not support struct".into()))
    }

    fn serialize_seq_start(&mut self, _len: Option<usize>) -> Result<()> {
        Err(Error::custom("does not support seq".into()))
    }

    fn serialize_seq_item(&mut self) -> Result<&mut dyn SimpleSerializer> {
        Err(Error::custom("does not support seq".into()))
    }

    fn serialize_seq_end(&mut self) -> Result<()> {
        Err(Error::custom("does not support seq".into()))
    }
}

struct Mut<'a, T: ?Sized>(&'a mut T);

impl<'a, T: ?Sized> std::ops::Deref for Mut<'a, T> {
    type Target = T;
    fn deref(&self) -> &Self::Target {
        self.0
    }
}

impl<'a, T: ?Sized> std::ops::DerefMut for Mut<'a, T> {
    fn deref_mut(&mut self) -> &mut Self::Target {
        self.0
    }
}

impl<T> serde::ser::Serializer for Mut<'_, T>
where
    T: ?Sized + SimpleSerializer,
{
    type Error = Error;
    type Ok = ();

    type SerializeStruct = Self;
    type SerializeSeq = Self;
    type SerializeTupleVariant = Impossible<(), Error>;
    type SerializeTupleStruct = Impossible<(), Error>;
    type SerializeTuple = Impossible<(), Error>;
    type SerializeStructVariant = Impossible<(), Error>;
    type SerializeMap = Impossible<(), Error>;

    fn serialize_bool(mut self, v: bool) -> Result<()> {
        SimpleSerializer::serialize_bool(&mut *self, v)
    }

    fn serialize_f32(mut self, v: f32) -> Result<()> {
        SimpleSerializer::serialize_f32(&mut *self, v)
    }

    fn serialize_f64(mut self, v: f64) -> Result<()> {
        SimpleSerializer::serialize_f64(&mut *self, v)
    }

    fn serialize_struct(mut self, name: &'static str, len: usize) -> Result<Self> {
        SimpleSerializer::serialize_struct_start(&mut *self, name, len)?;
        Ok(self)
    }

    fn serialize_char(self, _: char) -> Result<()> {
        todo!()
    }

    fn serialize_i8(self, _: i8) -> Result<()> {
        todo!()
    }

    fn serialize_i16(self, _: i16) -> Result<()> {
        todo!()
    }

    fn serialize_str(mut self, v: &str) -> Result<()> {
        SimpleSerializer::serialize_str(&mut *self, v)
    }

    fn serialize_unit_struct(self, _: &'static str) -> Result<()> {
        todo!()
    }

    fn serialize_unit_variant(self, _: &'static str, _: u32, _: &'static str) -> Result<()> {
        todo!()
    }

    fn serialize_i32(self, _: i32) -> Result<()> {
        todo!()
    }

    fn serialize_newtype_struct<V>(self, _: &'static str, _: &V) -> Result<()>
    where
        V: ?Sized + Serialize,
    {
        todo!()
    }

    fn serialize_newtype_variant<V>(
        self,
        _: &'static str,
        _: u32,
        _: &'static str,
        _: &V,
    ) -> Result<()>
    where
        V: ?Sized + Serialize,
    {
        todo!()
    }

    fn serialize_tuple_struct(
        self,
        _: &'static str,
        _: usize,
    ) -> Result<Self::SerializeTupleStruct> {
        todo!()
    }

    fn serialize_tuple_variant(
        self,
        _: &'static str,
        _: u32,
        _: &'static str,
        _: usize,
    ) -> Result<Self::SerializeTupleVariant> {
        todo!()
    }

    fn serialize_struct_variant(
        self,
        _: &'static str,
        _: u32,
        _: &'static str,
        _: usize,
    ) -> Result<Self::SerializeStructVariant> {
        todo!()
    }

    fn serialize_i64(self, _: i64) -> Result<()> {
        todo!()
    }

    fn serialize_u8(self, _: u8) -> Result<()> {
        todo!()
    }

    fn serialize_u16(self, _: u16) -> Result<()> {
        todo!()
    }

    fn serialize_u32(self, _: u32) -> Result<()> {
        todo!()
    }

    fn serialize_u64(self, _: u64) -> Result<()> {
        todo!()
    }

    fn serialize_bytes(self, _: &[u8]) -> Result<()> {
        todo!()
    }

    fn serialize_none(self) -> Result<()> {
        todo!()
    }

    fn serialize_some<V>(self, _: &V) -> Result<()>
    where
        V: ?Sized + Serialize,
    {
        todo!()
    }

    fn serialize_unit(self) -> Result<()> {
        todo!()
    }

    fn serialize_seq(mut self, len: Option<usize>) -> Result<Self::SerializeSeq, Self::Error> {
        SimpleSerializer::serialize_seq_start(&mut *self, len)?;
        Ok(self)
    }

    fn serialize_tuple(self, _: usize) -> Result<Self::SerializeTuple, Self::Error> {
        todo!()
    }

    fn serialize_map(self, _: Option<usize>) -> Result<Self::SerializeMap, Self::Error> {
        todo!()
    }
}

impl<T> serde::ser::SerializeStruct for Mut<'_, T>
where
    T: ?Sized + SimpleSerializer,
{
    type Ok = ();
    type Error = Error;

    fn serialize_field<V>(&mut self, key: &'static str, value: &V) -> Result<(), Self::Error>
    where
        V: ?Sized + Serialize,
    {
        value.serialize(Mut(SimpleSerializer::serialize_struct_field(
            &mut **self,
            key,
        )?))
    }

    fn end(mut self) -> Result<()> {
        SimpleSerializer::serialize_struct_end(&mut *self)
    }
}

impl<T: ?Sized + SimpleSerializer> serde::ser::SerializeSeq for Mut<'_, T> {
    type Ok = ();
    type Error = Error;

    fn serialize_element<V: ?Sized + Serialize>(&mut self, value: &V) -> Result<()> {
        value.serialize(Mut(SimpleSerializer::serialize_seq_item(&mut **self)?))
    }

    fn end(mut self) -> Result<()> {
        SimpleSerializer::serialize_seq_end(&mut *self)
    }
}
