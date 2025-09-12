#![allow(unused)]
use marrow::{
    array::{Array, BooleanArray, BytesArray, ListArray, PrimitiveArray, StructArray},
    datatypes::{DataType, Field, FieldMeta},
};
use rand::{
    distributions::{Standard, Uniform},
    prelude::Distribution,
    Rng,
};
use serde::{ser::Impossible, Serialize};
use serde_arrow::{schema::SchemaLike, Error, Result};

const NUM_REPETITIONS: usize = 1_000;

fn main() {
    let items = (0..100)
        .map(|_| Item::random(&mut rand::thread_rng()))
        .collect::<Vec<_>>();

    let fields = Vec::<Field>::from_samples(&items, Default::default()).unwrap();

    for _ in 0..NUM_REPETITIONS {
        let arrays = to_marrow_custom(&fields, &items).unwrap();
        criterion::black_box(arrays);
    }
}

#[derive(Debug, Serialize)]
pub struct Item {
    string: String,
    points: Vec<Point>,
    child: SubItem,
}

#[derive(Debug, Serialize)]
struct Point {
    x: f32,
    y: f32,
}

#[derive(Debug, Serialize)]
struct SubItem {
    a: bool,
    b: f64,
    // c: Option<f32>,
}

impl Item {
    pub fn random<R: Rng + ?Sized>(rng: &mut R) -> Self {
        let n_string = Uniform::new(1, 50).sample(rng);
        let n_points = Uniform::new(1, 50).sample(rng);

        Self {
            string: (0..n_string)
                .map(|_| -> char { Standard.sample(rng) })
                .collect(),
            points: (0..n_points)
                .map(|_| Point {
                    x: Standard.sample(rng),
                    y: Standard.sample(rng),
                })
                .collect(),
            child: SubItem {
                a: Standard.sample(rng),
                b: Standard.sample(rng),
                //c: Standard.sample(rng),
            },
        }
    }
}

fn to_marrow_custom<T: ?Sized + Serialize>(fields: &[Field], items: &T) -> Result<Vec<Array>> {
    let mut serializers = Vec::with_capacity(fields.len());
    for field in fields {
        serializers.push(build_serializer(field)?);
    }
    let mut serializer = OuterSerializer(StructSerializer::new(fields, serializers));
    items.serialize(Mut(&mut serializer))?;

    let mut result = Vec::new();
    for field in &mut serializer.0.serializers {
        result.push(field.build_array()?);
    }

    Ok(result)
}

struct OuterSerializer<'a>(StructSerializer<'a>);

impl<'a> SimpleSerializer for OuterSerializer<'a> {
    fn build_array(&mut self) -> Result<Array> {
        Err(Error::custom("cannot build arrays".into()))
    }

    fn serialize_seq_start(&mut self, len: Option<usize>) -> Result<()> {
        if let Some(len) = len {
            self.0.reserve(len);
        }
        Ok(())
    }

    fn serialize_seq_item(&mut self) -> Result<&mut dyn SimpleSerializer> {
        Ok(&mut self.0)
    }

    fn serialize_seq_end(&mut self) -> Result<()> {
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
    serializers: Vec<ArraySerializer<'a>>,
    next: usize,
    len: usize,
}

impl<'a> StructSerializer<'a> {
    pub fn new(fields: &'a [Field], serializers: Vec<ArraySerializer<'a>>) -> StructSerializer<'a> {
        Self {
            fields,
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
        if self.fields[current].name != key {
            return Err(Error::custom("Out of order fields".into()));
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
    fn reserve(&mut self, additional: usize) {}

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

    fn serialize_char(self, v: char) -> std::result::Result<Self::Ok, Self::Error> {
        Err(Error::custom("does not support char".into()))
    }

    fn serialize_i8(self, v: i8) -> std::result::Result<Self::Ok, Self::Error> {
        todo!()
    }

    fn serialize_i16(self, v: i16) -> std::result::Result<Self::Ok, Self::Error> {
        todo!()
    }

    fn serialize_str(mut self, v: &str) -> Result<()> {
        SimpleSerializer::serialize_str(&mut *self, v)
    }

    fn serialize_unit_struct(
        self,
        name: &'static str,
    ) -> std::result::Result<Self::Ok, Self::Error> {
        todo!()
    }

    fn serialize_unit_variant(
        self,
        name: &'static str,
        variant_index: u32,
        variant: &'static str,
    ) -> std::result::Result<Self::Ok, Self::Error> {
        todo!()
    }

    fn serialize_i32(self, v: i32) -> std::result::Result<Self::Ok, Self::Error> {
        todo!()
    }

    fn serialize_newtype_struct<V>(
        self,
        name: &'static str,
        value: &V,
    ) -> std::result::Result<Self::Ok, Self::Error>
    where
        V: ?Sized + Serialize,
    {
        todo!()
    }

    fn serialize_newtype_variant<V>(
        self,
        name: &'static str,
        variant_index: u32,
        variant: &'static str,
        value: &V,
    ) -> std::result::Result<Self::Ok, Self::Error>
    where
        V: ?Sized + Serialize,
    {
        todo!()
    }

    fn serialize_tuple_struct(
        self,
        name: &'static str,
        len: usize,
    ) -> std::result::Result<Self::SerializeTupleStruct, Self::Error> {
        todo!()
    }

    fn serialize_tuple_variant(
        self,
        name: &'static str,
        variant_index: u32,
        variant: &'static str,
        len: usize,
    ) -> std::result::Result<Self::SerializeTupleVariant, Self::Error> {
        todo!()
    }

    fn serialize_struct_variant(
        self,
        name: &'static str,
        variant_index: u32,
        variant: &'static str,
        len: usize,
    ) -> std::result::Result<Self::SerializeStructVariant, Self::Error> {
        todo!()
    }

    fn serialize_i64(self, v: i64) -> std::result::Result<Self::Ok, Self::Error> {
        todo!()
    }

    fn serialize_u8(self, v: u8) -> std::result::Result<Self::Ok, Self::Error> {
        todo!()
    }

    fn serialize_u16(self, v: u16) -> std::result::Result<Self::Ok, Self::Error> {
        todo!()
    }

    fn serialize_u32(self, v: u32) -> std::result::Result<Self::Ok, Self::Error> {
        todo!()
    }

    fn serialize_u64(self, v: u64) -> std::result::Result<Self::Ok, Self::Error> {
        todo!()
    }

    fn serialize_bytes(self, v: &[u8]) -> std::result::Result<Self::Ok, Self::Error> {
        todo!()
    }

    fn serialize_none(self) -> std::result::Result<Self::Ok, Self::Error> {
        todo!()
    }

    fn serialize_some<V>(self, value: &V) -> std::result::Result<Self::Ok, Self::Error>
    where
        V: ?Sized + Serialize,
    {
        todo!()
    }

    fn serialize_unit(self) -> std::result::Result<Self::Ok, Self::Error> {
        todo!()
    }

    fn serialize_seq(
        mut self,
        len: Option<usize>,
    ) -> std::result::Result<Self::SerializeSeq, Self::Error> {
        SimpleSerializer::serialize_seq_start(&mut *self, len)?;
        Ok(self)
    }

    fn serialize_tuple(self, len: usize) -> std::result::Result<Self::SerializeTuple, Self::Error> {
        todo!()
    }

    fn serialize_map(
        self,
        len: Option<usize>,
    ) -> std::result::Result<Self::SerializeMap, Self::Error> {
        todo!()
    }
}

impl<T> serde::ser::SerializeStruct for Mut<'_, T>
where
    T: ?Sized + SimpleSerializer,
{
    type Ok = ();
    type Error = Error;

    fn serialize_field<V>(
        &mut self,
        key: &'static str,
        value: &V,
    ) -> std::result::Result<(), Self::Error>
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
