//! An implementation using static dispatch via enums
use std::marker::PhantomData;

use marrow::{
    array::{Array, BooleanArray, BytesArray, ListArray, PrimitiveArray, StructArray},
    datatypes::{DataType, Field, FieldMeta},
};
use serde::Serialize;
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
    let mut serializer = OuterSerializer {
        field_names: vec![None; fields.len()],
        fields,
        serializers,
    };
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
    serializers: Vec<ArraySerializer<'a>>,
}

impl<'s, 'a> serde::Serializer for &'s mut OuterSerializer<'a> {
    type Ok = ();
    type Error = Error;

    type SerializeSeq = Self;

    fn serialize_seq(self, len: Option<usize>) -> Result<Self::SerializeSeq> {
        if let Some(len) = len {
            for serializer in &mut self.serializers {
                serializer.reserve(len);
            }
        }
        Ok(self)
    }

    unsupported!(
        serialize_unit,
        serialize_bool,
        serialize_i8,
        serialize_i16,
        serialize_i32,
        serialize_i64,
        serialize_u8,
        serialize_u16,
        serialize_u32,
        serialize_u64,
        serialize_f32,
        serialize_f64,
        serialize_some,
        serialize_none,
        serialize_struct,
        serialize_tuple_struct,
        serialize_unit_variant,
        serialize_tuple_variant,
        serialize_newtype_struct,
        serialize_newtype_variant,
        serialize_unit_struct,
        serialize_struct_variant,
        serialize_str,
        serialize_char,
        serialize_bytes,
        serialize_map,
        serialize_tuple,
    );
}

impl<'s, 'a> serde::ser::SerializeSeq for &'s mut OuterSerializer<'a> {
    type Ok = ();
    type Error = Error;

    fn serialize_element<T: ?Sized + Serialize>(&mut self, value: &T) -> Result<()> {
        value.serialize(OuterSerializerStruct {
            fields: self.fields,
            serializers: &mut self.serializers,
            field_names: &mut self.field_names,
            current: 0,
        })
    }

    fn end(self) -> Result<()> {
        Ok(())
    }
}

struct OuterSerializerStruct<'s, 'a> {
    fields: &'a [Field],
    serializers: &'s mut [ArraySerializer<'a>],
    field_names: &'s mut [Option<StaticFieldName>],
    current: usize,
}

impl<'s, 'a> serde::Serializer for OuterSerializerStruct<'s, 'a> {
    type Ok = ();
    type Error = Error;

    type SerializeStruct = Self;

    fn serialize_struct(self, _name: &'static str, _len: usize) -> Result<Self::SerializeStruct> {
        Ok(self)
    }

    unsupported!(
        serialize_unit,
        serialize_bool,
        serialize_i8,
        serialize_i16,
        serialize_i32,
        serialize_i64,
        serialize_u8,
        serialize_u16,
        serialize_u32,
        serialize_u64,
        serialize_f32,
        serialize_f64,
        serialize_some,
        serialize_none,
        serialize_tuple_struct,
        serialize_unit_variant,
        serialize_tuple_variant,
        serialize_newtype_struct,
        serialize_newtype_variant,
        serialize_unit_struct,
        serialize_struct_variant,
        serialize_str,
        serialize_char,
        serialize_bytes,
        serialize_map,
        serialize_tuple,
        serialize_seq,
    );
}

impl<'s, 'a> serde::ser::SerializeStruct for OuterSerializerStruct<'s, 'a> {
    type Ok = ();
    type Error = Error;

    fn serialize_field<T: ?Sized + Serialize>(
        &mut self,
        key: &'static str,
        value: &T,
    ) -> Result<()> {
        let current = self.current;
        self.current += 1;

        if self.field_names.get(current).copied() != Some(Some(StaticFieldName::new(key))) {
            if self.fields[current].name != key {
                return Err(Error::custom("Out of order fields".into()));
            }
            self.field_names[current] = Some(StaticFieldName::new(key));
        }

        value.serialize(&mut self.serializers[current])
    }

    fn end(self) -> Result<()> {
        if self.current != self.fields.len() {
            return Err(Error::custom("Skipping fields is not supported".into()));
        }
        Ok(())
    }
}

enum ArraySerializer<'a> {
    Boolean(BoolSerializer<'a>),
    Float32(PrimitiveSerializer<'a, f32>),
    Float64(PrimitiveSerializer<'a, f64>),
    LargeUtf8(Utf8Serializer<'a>),
    Struct(StructSerializer<'a>),
    LargeList(SeqSerializer<'a>),
}

macro_rules! dispatch_array_serializer {
    ($obj:expr, $ty:ident($var:ident) => $block:expr) => {
        match $obj {
            $ty::Boolean($var) => $block,
            $ty::Float32($var) => $block,
            $ty::Float64($var) => $block,
            $ty::LargeUtf8($var) => $block,
            $ty::Struct($var) => $block,
            $ty::LargeList($var) => $block,
        }
    };
}

macro_rules! implement_array_serializer {
    ($s:lifetime, $a:lifetime; $($ident:ident),* $(,)?) => {
        type Ok = ();
        type Error = Error;

        type SerializeStruct = ArraySerializerStruct<$s, $a>;
        type SerializeSeq = ArraySerializerSeq<$s, $a>;

        $( implement_array_serializer!(impl $ident); )*
    };
    (impl serialize_struct) => {
        fn serialize_struct(self, _name: &'static str, _len: usize) -> Result<Self::SerializeStruct> {
            Err(Error::custom("cannot serialize struct".into()))
        }
    };
    (impl serialize_seq) => {
        fn serialize_seq(self, _len: Option<usize>) -> Result<Self::SerializeSeq> {
            Err(Error::custom("cannot serialize seq".into()))
        }
    };
    (impl $ident:ident) => {
        unsupported!(impl $ident);
    };
}

impl<'a> ArraySerializer<'a> {
    fn reserve(&mut self, additional: usize) {
        dispatch_array_serializer!(self, Self(ser) => ser.reserve(additional))
    }

    fn build_array(&mut self) -> Result<Array> {
        dispatch_array_serializer!(self, Self(ser) => ser.build_array())
    }
}

impl<'s, 'a> serde::Serializer for &'s mut ArraySerializer<'a> {
    fn serialize_bool(self, v: bool) -> Result<()> {
        dispatch_array_serializer!(self, ArraySerializer(ser) => ser.serialize_bool(v))
    }

    fn serialize_f32(self, v: f32) -> Result<()> {
        dispatch_array_serializer!(self, ArraySerializer(ser) => ser.serialize_f32(v))
    }

    fn serialize_f64(self, v: f64) -> Result<()> {
        dispatch_array_serializer!(self, ArraySerializer(ser) => ser.serialize_f64(v))
    }

    fn serialize_str(self, v: &str) -> Result<()> {
        dispatch_array_serializer!(self, ArraySerializer(ser) => ser.serialize_str(v))
    }

    fn serialize_struct(self, name: &'static str, len: usize) -> Result<Self::SerializeStruct> {
        dispatch_array_serializer!(self, ArraySerializer(ser) => ser.serialize_struct(name, len))
    }

    fn serialize_seq(self, len: Option<usize>) -> Result<Self::SerializeSeq> {
        dispatch_array_serializer!(self, ArraySerializer(ser) => ser.serialize_seq(len))
    }

    implement_array_serializer!(
        's, 'a;
        serialize_unit,
        serialize_i8,
        serialize_i16,
        serialize_i32,
        serialize_i64,
        serialize_u8,
        serialize_u16,
        serialize_u32,
        serialize_u64,
        serialize_some,
        serialize_none,
        serialize_tuple_struct,
        serialize_unit_variant,
        serialize_tuple_variant,
        serialize_newtype_struct,
        serialize_newtype_variant,
        serialize_unit_struct,
        serialize_struct_variant,
        serialize_char,
        serialize_bytes,
        serialize_map,
        serialize_tuple,
    );
}

struct ArraySerializerStruct<'s, 'a> {
    fields: &'a [Field],
    serializers: &'s mut [ArraySerializer<'a>],
    field_names: &'s mut [Option<StaticFieldName>],
    current: usize,
}

impl<'s, 'a> serde::ser::SerializeStruct for ArraySerializerStruct<'s, 'a> {
    type Ok = ();
    type Error = Error;

    fn serialize_field<T: ?Sized + Serialize>(
        &mut self,
        key: &'static str,
        value: &T,
    ) -> Result<()> {
        let current = self.current;
        self.current += 1;

        if self.field_names.get(current).copied() != Some(Some(StaticFieldName::new(key))) {
            if self.fields[current].name != key {
                return Err(Error::custom("Out of order fields".into()));
            }
            self.field_names[current] = Some(StaticFieldName::new(key));
        }

        value.serialize(&mut self.serializers[current])
    }

    fn end(self) -> Result<()> {
        if self.current != self.serializers.len() {
            return Err(Error::custom("Skipping fields is not supported".into()));
        }
        Ok(())
    }
}

struct ArraySerializerSeq<'s, 'a> {
    offsets: &'s mut Vec<i64>,
    element: &'s mut ArraySerializer<'a>,
}

impl<'s, 'a> serde::ser::SerializeSeq for ArraySerializerSeq<'s, 'a> {
    type Ok = ();
    type Error = Error;

    fn serialize_element<T: ?Sized + Serialize>(&mut self, value: &T) -> Result<()> {
        value.serialize(&mut *self.element)?;
        let Some(last) = self.offsets.last_mut() else {
            return Err(Error::custom("Invalid offset array".into()));
        };
        *last += 1;
        Ok(())
    }

    fn end(self) -> Result<()> {
        Ok(())
    }
}

fn build_serializer<'a>(field: &'a Field) -> Result<ArraySerializer<'a>> {
    match &field.data_type {
        DataType::Boolean => Ok(ArraySerializer::Boolean(BoolSerializer::new())),
        DataType::Float32 => Ok(ArraySerializer::Float32(PrimitiveSerializer::<f32>::new())),
        DataType::Float64 => Ok(ArraySerializer::Float64(PrimitiveSerializer::<f64>::new())),
        DataType::LargeUtf8 => Ok(ArraySerializer::LargeUtf8(Utf8Serializer::new())),
        DataType::Struct(fields) => {
            let mut serializers = Vec::with_capacity(fields.len());
            for field in fields {
                serializers.push(build_serializer(field)?);
            }
            Ok(ArraySerializer::Struct(StructSerializer::new(
                fields,
                serializers,
            )))
        }
        DataType::LargeList(element) => {
            let serializer = build_serializer(element)?;
            Ok(ArraySerializer::LargeList(SeqSerializer::new(
                field, serializer,
            )))
        }
        dt => Err(Error::custom(format!("Unkown data type {dt:?}"))),
    }
}

struct PrimitiveSerializer<'a, T> {
    values: Vec<T>,
    fields: PhantomData<&'a [Field]>,
}

impl<'a, T> PrimitiveSerializer<'a, T> {
    pub fn new() -> Self {
        Self {
            values: Default::default(),
            fields: Default::default(),
        }
    }

    pub fn reserve(&mut self, additional: usize) {
        self.values.reserve(additional);
    }
}

impl<'a> PrimitiveSerializer<'a, f32> {
    fn build_array(&mut self) -> Result<Array> {
        Ok(Array::Float32(PrimitiveArray {
            validity: None,
            values: std::mem::take(&mut self.values),
        }))
    }
}

impl<'s, 'a> serde::Serializer for &'s mut PrimitiveSerializer<'a, f32> {
    fn serialize_f32(self, v: f32) -> Result<()> {
        self.values.push(v);
        Ok(())
    }

    implement_array_serializer!(
        's, 'a;
        serialize_unit,
        serialize_bool,
        serialize_i8,
        serialize_i16,
        serialize_i32,
        serialize_i64,
        serialize_u8,
        serialize_u16,
        serialize_u32,
        serialize_u64,
        serialize_f64,
        serialize_str,
        serialize_some,
        serialize_none,
        serialize_tuple_struct,
        serialize_unit_variant,
        serialize_tuple_variant,
        serialize_newtype_struct,
        serialize_newtype_variant,
        serialize_unit_struct,
        serialize_struct_variant,
        serialize_char,
        serialize_bytes,
        serialize_map,
        serialize_tuple,
        serialize_seq,
        serialize_struct,
    );
}

impl<'a> PrimitiveSerializer<'a, f64> {
    fn build_array(&mut self) -> Result<Array> {
        Ok(Array::Float64(PrimitiveArray {
            validity: None,
            values: std::mem::take(&mut self.values),
        }))
    }
}

impl<'s, 'a> serde::Serializer for &'s mut PrimitiveSerializer<'a, f64> {
    fn serialize_f64(self, v: f64) -> Result<()> {
        self.values.push(v);
        Ok(())
    }

    implement_array_serializer!(
        's, 'a;
        serialize_unit,
        serialize_bool,
        serialize_i8,
        serialize_i16,
        serialize_i32,
        serialize_i64,
        serialize_u8,
        serialize_u16,
        serialize_u32,
        serialize_u64,
        serialize_f32,
        serialize_str,
        serialize_some,
        serialize_none,
        serialize_tuple_struct,
        serialize_unit_variant,
        serialize_tuple_variant,
        serialize_newtype_struct,
        serialize_newtype_variant,
        serialize_unit_struct,
        serialize_struct_variant,
        serialize_char,
        serialize_bytes,
        serialize_map,
        serialize_tuple,
        serialize_seq,
        serialize_struct,
    );
}

#[derive(Default)]
struct BoolSerializer<'a> {
    len: usize,
    values: Vec<u8>,
    fields: PhantomData<&'a [Field]>,
}

impl<'a> BoolSerializer<'a> {
    pub fn new() -> Self {
        Self::default()
    }

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
}

impl<'s, 'a> serde::Serializer for &'s mut BoolSerializer<'a> {
    fn serialize_bool(self, v: bool) -> Result<()> {
        marrow::bits::push(&mut self.values, &mut self.len, v);
        Ok(())
    }

    implement_array_serializer!(
        's, 'a;
        serialize_unit,
        serialize_i8,
        serialize_i16,
        serialize_i32,
        serialize_i64,
        serialize_u8,
        serialize_u16,
        serialize_u32,
        serialize_u64,
        serialize_f32,
        serialize_f64,
        serialize_str,
        serialize_some,
        serialize_none,
        serialize_tuple_struct,
        serialize_unit_variant,
        serialize_tuple_variant,
        serialize_newtype_struct,
        serialize_newtype_variant,
        serialize_unit_struct,
        serialize_struct_variant,
        serialize_char,
        serialize_bytes,
        serialize_map,
        serialize_tuple,
        serialize_seq,
        serialize_struct,
    );
}

struct Utf8Serializer<'a> {
    offsets: Vec<i64>,
    data: Vec<u8>,
    fields: PhantomData<&'a [Field]>,
}

impl<'a> Utf8Serializer<'a> {
    pub fn new() -> Self {
        Self {
            offsets: vec![0],
            data: Vec::new(),
            fields: Default::default(),
        }
    }

    pub fn build_array(&mut self) -> Result<Array> {
        Ok(Array::LargeUtf8(BytesArray {
            validity: None,
            offsets: std::mem::replace(&mut self.offsets, vec![0]),
            data: std::mem::take(&mut self.data),
        }))
    }

    pub fn reserve(&mut self, additional: usize) {
        self.offsets.reserve(additional);
        self.data.reserve(additional * 8);
    }
}

impl<'s, 'a> serde::Serializer for &'s mut Utf8Serializer<'a> {
    fn serialize_str(self, v: &str) -> Result<()> {
        let Some(offset) = self.offsets.last() else {
            return Err(Error::custom("Invalid offset array".into()));
        };
        self.offsets.push(*offset + i64::try_from(v.len())?);
        self.data.extend(v.as_bytes());
        Ok(())
    }

    implement_array_serializer!(
        's, 'a;
        serialize_bool,
        serialize_unit,
        serialize_i8,
        serialize_i16,
        serialize_i32,
        serialize_i64,
        serialize_u8,
        serialize_u16,
        serialize_u32,
        serialize_u64,
        serialize_f32,
        serialize_f64,
        serialize_some,
        serialize_none,
        serialize_tuple_struct,
        serialize_unit_variant,
        serialize_tuple_variant,
        serialize_newtype_struct,
        serialize_newtype_variant,
        serialize_unit_struct,
        serialize_struct_variant,
        serialize_char,
        serialize_bytes,
        serialize_map,
        serialize_tuple,
        serialize_seq,
        serialize_struct,
    );
}

struct SeqSerializer<'a> {
    offsets: Vec<i64>,
    field: &'a Field,
    serializer: Box<ArraySerializer<'a>>,
}

impl<'a> SeqSerializer<'a> {
    pub fn new(field: &'a Field, serializer: ArraySerializer<'a>) -> Self {
        Self {
            offsets: vec![0],
            field,
            serializer: Box::new(serializer),
        }
    }

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
}

impl<'s, 'a> serde::Serializer for &'s mut SeqSerializer<'a> {
    fn serialize_seq(
        self,
        len: Option<usize>,
    ) -> std::result::Result<Self::SerializeSeq, Self::Error> {
        if let Some(len) = len {
            self.serializer.reserve(len);
        }
        let Some(last) = self.offsets.last() else {
            return Err(Error::custom("invalid offset array".into()));
        };
        self.offsets.push(*last);

        Ok(ArraySerializerSeq {
            offsets: &mut self.offsets,
            element: &mut self.serializer,
        })
    }

    implement_array_serializer!(
        's, 'a;
        serialize_bool,
        serialize_unit,
        serialize_i8,
        serialize_i16,
        serialize_i32,
        serialize_i64,
        serialize_u8,
        serialize_u16,
        serialize_u32,
        serialize_u64,
        serialize_f32,
        serialize_f64,
        serialize_some,
        serialize_none,
        serialize_tuple_struct,
        serialize_unit_variant,
        serialize_tuple_variant,
        serialize_newtype_struct,
        serialize_newtype_variant,
        serialize_unit_struct,
        serialize_struct_variant,
        serialize_char,
        serialize_str,
        serialize_bytes,
        serialize_map,
        serialize_tuple,
        serialize_struct,
    );
}

struct StructSerializer<'a> {
    fields: &'a [Field],
    field_names: Vec<Option<StaticFieldName>>,
    serializers: Vec<ArraySerializer<'a>>,
    len: usize,
}

impl<'a> StructSerializer<'a> {
    pub fn new(fields: &'a [Field], serializers: Vec<ArraySerializer<'a>>) -> StructSerializer<'a> {
        Self {
            fields,
            field_names: vec![None; fields.len()],
            serializers,
            len: 0,
        }
    }

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
}
impl<'s, 'a> serde::Serializer for &'s mut StructSerializer<'a> {
    fn serialize_struct(self, _name: &'static str, _len: usize) -> Result<Self::SerializeStruct> {
        self.len += 1;
        Ok(ArraySerializerStruct {
            fields: self.fields,
            serializers: &mut self.serializers,
            field_names: &mut self.field_names,
            current: 0,
        })
    }

    implement_array_serializer!(
        's, 'a;
        serialize_bool,
        serialize_unit,
        serialize_i8,
        serialize_i16,
        serialize_i32,
        serialize_i64,
        serialize_u8,
        serialize_u16,
        serialize_u32,
        serialize_u64,
        serialize_f32,
        serialize_f64,
        serialize_some,
        serialize_none,
        serialize_tuple_struct,
        serialize_unit_variant,
        serialize_tuple_variant,
        serialize_newtype_struct,
        serialize_newtype_variant,
        serialize_unit_struct,
        serialize_struct_variant,
        serialize_char,
        serialize_str,
        serialize_bytes,
        serialize_map,
        serialize_tuple,
        serialize_seq,
    );
}
