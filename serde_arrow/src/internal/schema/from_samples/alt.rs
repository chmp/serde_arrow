//! Alternative from_samples impl directly using Serialize
//!

use serde::{ser::Impossible, Serialize};

use crate::internal::{
    error::{fail, Error, Result},
    schema::{
        tracer::{
            ListTracer, MapTracer, StructMode, StructTracer, Tracer, TupleTracer, UnionVariant,
        },
        TracingMode, TracingOptions,
    },
};

pub fn to_tracer<T: Serialize + ?Sized>(items: &T, options: TracingOptions) -> Result<Tracer> {
    let options = options.tracing_mode(TracingMode::FromSamples);
    let mut tracer = Tracer::new("$".into(), options);
    items.serialize(OuterSequenceSerializer(&mut tracer))?;
    tracer.finish()?;
    Ok(tracer)
}

macro_rules! unimplemented_fn {
    ($name:ident $($args:tt)* ) => {
        fn $name $($args)* {
            fail!("Invalid argument: from_sample expects a sequence of records as its argument");
        }
    };
}

struct OuterSequenceSerializer<'a>(&'a mut Tracer);

#[rustfmt::skip]
impl<'a> serde::ser::Serializer for OuterSequenceSerializer<'a> {
    type Ok = ();
    type Error = Error;

    type SerializeSeq = Self;
    type SerializeTuple = Self;
    type SerializeTupleVariant = Self;

    fn serialize_seq(self, _: Option<usize>) -> Result<Self::SerializeSeq> {
        Ok(self)
    }
    fn serialize_tuple(self, _: usize) -> Result<Self::SerializeTuple> {
        Ok(self)
    }

    fn serialize_tuple_variant(self, _: &'static str, _: u32, _: &'static str, _: usize) -> Result<Self::SerializeTupleVariant> {
        Ok(self)
    }

    type SerializeMap = Impossible<Self::Ok, Self::Error>;
    type SerializeStruct = Impossible<Self::Ok, Self::Error>;
    type SerializeStructVariant = Impossible<Self::Ok, Self::Error>;
    type SerializeTupleStruct = Impossible<Self::Ok, Self::Error>;

    unimplemented_fn!(serialize_bool(self, _: bool) -> Result<Self::Ok>);
    unimplemented_fn!(serialize_i8(self, _: i8) -> Result<Self::Ok>);
    unimplemented_fn!(serialize_i16(self, _: i16) -> Result<Self::Ok>);
    unimplemented_fn!(serialize_i32(self, _: i32) -> Result<Self::Ok>);
    unimplemented_fn!(serialize_i64(self, _: i64) -> Result<Self::Ok>);
    unimplemented_fn!(serialize_u8(self, _: u8) -> Result<Self::Ok>);
    unimplemented_fn!(serialize_u16(self, _: u16) -> Result<Self::Ok>);
    unimplemented_fn!(serialize_u32(self, _: u32) -> Result<Self::Ok>);
    unimplemented_fn!(serialize_u64(self, _: u64) -> Result<Self::Ok>);
    unimplemented_fn!(serialize_f32(self, _: f32) -> Result<Self::Ok>);
    unimplemented_fn!(serialize_f64(self, _: f64) -> Result<Self::Ok>);
    unimplemented_fn!(serialize_char(self, _: char) -> Result<Self::Ok>);
    unimplemented_fn!(serialize_unit(self) -> Result<Self::Ok>);
    unimplemented_fn!(serialize_str(self, _: &str) -> Result<Self::Ok>);
    unimplemented_fn!(serialize_bytes(self, _: &[u8]) -> Result<Self::Ok>);
    unimplemented_fn!(serialize_none(self) -> Result<Self::Ok>);
    unimplemented_fn!(serialize_map(self, _: Option<usize>) -> Result<Self::SerializeMap>);
    unimplemented_fn!(serialize_struct(self, _: &'static str, _: usize) -> Result<Self::SerializeStruct>);
    unimplemented_fn!(serialize_struct_variant(self, _: &'static str, _: u32, _: &'static str, _: usize) -> Result<Self::SerializeStructVariant>);
    unimplemented_fn!(serialize_tuple_struct(self, _: &'static str, _: usize) -> Result<Self::SerializeTupleStruct>);
    unimplemented_fn!(serialize_unit_struct(self, _: &'static str) -> Result<Self::Ok>);
    unimplemented_fn!(serialize_unit_variant(self, _: &'static str, _: u32, _: &'static str) -> Result<Self::Ok>);
    unimplemented_fn!(serialize_some<T: Serialize + ?Sized>(self, _: &T) -> Result<Self::Ok>);
    unimplemented_fn!(serialize_newtype_struct<T: Serialize + ?Sized>(self, _: &'static str, _: &T) -> Result<Self::Ok>);
    unimplemented_fn!(serialize_newtype_variant<T: Serialize + ?Sized>(self, _: &'static str, _: u32, _: &'static str, _: &T) -> Result<Self::Ok>);
}

impl<'a> serde::ser::SerializeSeq for OuterSequenceSerializer<'a> {
    type Ok = ();
    type Error = Error;

    fn serialize_element<T: Serialize + ?Sized>(&mut self, value: &T) -> Result<()> {
        value.serialize(TracerSerializer(&mut *self.0))
    }

    fn end(self) -> Result<Self::Ok> {
        Ok(())
    }
}

impl<'a> serde::ser::SerializeTuple for OuterSequenceSerializer<'a> {
    type Ok = ();
    type Error = Error;

    fn serialize_element<T: Serialize + ?Sized>(&mut self, value: &T) -> Result<()> {
        value.serialize(TracerSerializer(&mut *self.0))
    }

    fn end(self) -> std::prelude::v1::Result<Self::Ok, Self::Error> {
        Ok(())
    }
}

impl<'a> serde::ser::SerializeTupleVariant for OuterSequenceSerializer<'a> {
    type Ok = ();
    type Error = Error;

    fn serialize_field<T: Serialize + ?Sized>(&mut self, value: &T) -> Result<()> {
        value.serialize(TracerSerializer(&mut *self.0))
    }

    fn end(self) -> Result<Self::Ok> {
        Ok(())
    }
}

struct TracerSerializer<'a>(&'a mut Tracer);

impl<'a> TracerSerializer<'a> {
    fn ensure_union_variant(
        self,
        variant_name: &str,
        variant_index: u32,
    ) -> Result<&'a mut UnionVariant> {
        self.0.ensure_union(&[])?;
        let Tracer::Union(tracer) = self.0 else {
            unreachable!();
        };
        let variant_index: usize = variant_index.try_into()?;
        tracer.ensure_variant(variant_name, variant_index)?;
        let Some(variant) = &mut tracer.variants[variant_index] else {
            unreachable!();
        };
        Ok(variant)
    }
}

impl<'a> serde::ser::Serializer for TracerSerializer<'a> {
    type Ok = ();
    type Error = Error;

    type SerializeStruct = StructSerializer<'a>;
    type SerializeMap = MapSerializer<'a>;
    type SerializeSeq = ListSerializer<'a>;
    type SerializeTuple = TupleSerializer<'a>;
    type SerializeTupleStruct = TupleSerializer<'a>;
    type SerializeStructVariant = Impossible<(), Error>;
    type SerializeTupleVariant = Impossible<(), Error>;

    fn serialize_bool(self, _: bool) -> Result<Self::Ok> {
        self.0.ensure_bool()
    }

    fn serialize_i8(self, _: i8) -> Result<Self::Ok, Self::Error> {
        self.0.ensure_i8()
    }

    fn serialize_i16(self, _: i16) -> Result<Self::Ok, Self::Error> {
        self.0.ensure_i16()
    }

    fn serialize_i32(self, _: i32) -> Result<Self::Ok, Self::Error> {
        self.0.ensure_i32()
    }

    fn serialize_i64(self, _: i64) -> Result<Self::Ok, Self::Error> {
        self.0.ensure_i64()
    }

    fn serialize_u8(self, _: u8) -> Result<Self::Ok, Self::Error> {
        self.0.ensure_u8()
    }

    fn serialize_u16(self, _: u16) -> Result<Self::Ok, Self::Error> {
        self.0.ensure_u16()
    }

    fn serialize_u32(self, _: u32) -> Result<Self::Ok, Self::Error> {
        self.0.ensure_u32()
    }

    fn serialize_u64(self, _: u64) -> Result<Self::Ok, Self::Error> {
        self.0.ensure_u64()
    }

    fn serialize_f32(self, _: f32) -> Result<Self::Ok, Self::Error> {
        self.0.ensure_f32()
    }

    fn serialize_f64(self, _: f64) -> Result<Self::Ok, Self::Error> {
        self.0.ensure_f64()
    }

    fn serialize_char(self, v: char) -> Result<Self::Ok, Self::Error> {
        self.0.ensure_u32()
    }

    fn serialize_unit(self) -> Result<Self::Ok, Self::Error> {
        self.0.ensure_null()
    }

    fn serialize_str(self, v: &str) -> Result<Self::Ok, Self::Error> {
        self.0.ensure_utf8()
    }

    fn serialize_bytes(self, v: &[u8]) -> Result<Self::Ok, Self::Error> {
        fail!("cannot trace bytes")
    }

    fn serialize_none(self) -> Result<Self::Ok, Self::Error> {
        self.0.mark_nullable();
        Ok(())
    }

    fn serialize_some<T: Serialize + ?Sized>(self, value: &T) -> Result<Self::Ok, Self::Error> {
        self.0.mark_nullable();
        value.serialize(self)
    }

    fn serialize_unit_struct(self, _: &'static str) -> Result<Self::Ok> {
        self.serialize_unit()
    }

    fn serialize_newtype_struct<T: Serialize + ?Sized>(
        self,
        _: &'static str,
        value: &T,
    ) -> Result<Self::Ok> {
        value.serialize(self)
    }

    fn serialize_map(self, _: Option<usize>) -> Result<Self::SerializeMap, Self::Error> {
        if self.0.get_options().map_as_struct {
            self.0.ensure_struct::<&str>(&[], StructMode::Map)?;
            let Tracer::Struct(tracer) = self.0 else {
                unreachable!();
            };
            Ok(MapSerializer::AsStruct(tracer, None))
        } else {
            self.0.ensure_map()?;
            let Tracer::Map(tracer) = self.0 else {
                unreachable!();
            };
            Ok(MapSerializer::AsMap(tracer))
        }
    }

    fn serialize_seq(self, len: Option<usize>) -> Result<Self::SerializeSeq> {
        self.0.ensure_list()?;
        let Tracer::List(tracer) = self.0 else {
            unreachable!();
        };
        Ok(ListSerializer(tracer))
    }

    fn serialize_struct(self, _: &'static str, _: usize) -> Result<Self::SerializeStruct> {
        self.0.ensure_struct::<&str>(&[], StructMode::Struct)?;
        let Tracer::Struct(tracer) = self.0 else {
            unreachable!();
        };
        Ok(StructSerializer(tracer))
    }

    fn serialize_tuple(self, len: usize) -> Result<Self::SerializeTuple> {
        self.0.ensure_tuple(len)?;
        let Tracer::Tuple(tracer) = self.0 else {
            unreachable!();
        };
        Ok(TupleSerializer::new(tracer))
    }

    fn serialize_tuple_struct(
        self,
        name: &'static str,
        len: usize,
    ) -> Result<Self::SerializeTupleStruct> {
        self.0.ensure_tuple(len)?;
        let Tracer::Tuple(tracer) = self.0 else {
            unreachable!();
        };
        Ok(TupleSerializer::new(tracer))
    }

    fn serialize_unit_variant(
        mut self,
        _: &'static str,
        variant_index: u32,
        variant_name: &'static str,
    ) -> Result<Self::Ok> {
        let variant = self.ensure_union_variant(variant_name, variant_index)?;
        variant.tracer.ensure_null()
    }

    fn serialize_newtype_variant<T: Serialize + ?Sized>(
        self,
        _: &'static str,
        variant_index: u32,
        variant_name: &'static str,
        value: &T,
    ) -> Result<Self::Ok> {
        let variant = self.ensure_union_variant(variant_name, variant_index)?;
        value.serialize(TracerSerializer(&mut variant.tracer))
    }

    fn serialize_struct_variant(
        self,
        name: &'static str,
        variant_index: u32,
        variant: &'static str,
        len: usize,
    ) -> Result<Self::SerializeStructVariant> {
        fail!("serialize_struct_variant is not implemented")
    }

    fn serialize_tuple_variant(
        self,
        name: &'static str,
        variant_index: u32,
        variant: &'static str,
        len: usize,
    ) -> Result<Self::SerializeTupleVariant> {
        fail!("serialize_tuple_variant is not implemented")
    }
}

struct StructSerializer<'a>(&'a mut StructTracer);

impl<'a> serde::ser::SerializeStruct for StructSerializer<'a> {
    type Ok = ();
    type Error = Error;

    fn serialize_field<T: Serialize + ?Sized>(
        &mut self,
        key: &'static str,
        value: &T,
    ) -> Result<()> {
        let field_idx = self.0.ensure_field(key)?;
        let Some(field_tracer) = self.0.get_field_tracer_mut(field_idx) else {
            unreachable!();
        };
        value.serialize(TracerSerializer(field_tracer))
    }

    fn end(self) -> Result<Self::Ok> {
        self.0.end()
    }
}

struct ListSerializer<'a>(&'a mut ListTracer);

impl<'a> serde::ser::SerializeSeq for ListSerializer<'a> {
    type Ok = ();
    type Error = Error;

    fn serialize_element<T: Serialize + ?Sized>(&mut self, value: &T) -> Result<()> {
        value.serialize(TracerSerializer(&mut self.0.item_tracer))
    }

    fn end(self) -> Result<Self::Ok> {
        Ok(())
    }
}

struct TupleSerializer<'a>(&'a mut TupleTracer, usize);

impl<'a> TupleSerializer<'a> {
    fn new(tracer: &'a mut TupleTracer) -> Self {
        Self(tracer, 0)
    }
}

impl<'a> serde::ser::SerializeTuple for TupleSerializer<'a> {
    type Ok = ();
    type Error = Error;

    fn serialize_element<T: Serialize + ?Sized>(&mut self, value: &T) -> Result<()> {
        let pos = self.1;
        value.serialize(TracerSerializer(self.0.field_tracer(pos)))?;
        self.1 += 1;
        Ok(())
    }

    fn end(self) -> std::prelude::v1::Result<Self::Ok, Self::Error> {
        Ok(())
    }
}

impl<'a> serde::ser::SerializeTupleStruct for TupleSerializer<'a> {
    type Ok = ();
    type Error = Error;

    fn serialize_field<T: Serialize + ?Sized>(&mut self, value: &T) -> Result<()> {
        let pos = self.1;
        value.serialize(TracerSerializer(self.0.field_tracer(pos)))?;
        self.1 += 1;
        Ok(())
    }

    fn end(self) -> std::prelude::v1::Result<Self::Ok, Self::Error> {
        Ok(())
    }
}

enum MapSerializer<'a> {
    AsStruct(&'a mut StructTracer, Option<String>),
    AsMap(&'a mut MapTracer),
}

impl<'a> serde::ser::SerializeMap for MapSerializer<'a> {
    type Ok = ();
    type Error = Error;

    fn serialize_key<T: Serialize + ?Sized>(&mut self, key: &T) -> Result<()> {
        match self {
            Self::AsStruct(_, next_key) => {
                *next_key = Some(key.serialize(SerializeToString)?);
                Ok(())
            }
            Self::AsMap(tracer) => key.serialize(TracerSerializer(&mut tracer.key_tracer)),
        }
    }

    fn serialize_value<T: Serialize + ?Sized>(&mut self, value: &T) -> Result<()> {
        match self {
            Self::AsStruct(tracer, next_key) => {
                let Some(next_key) = next_key.take() else {
                    fail!("invalid operations");
                };
                let field_idx = tracer.ensure_field(&next_key)?;
                let Some(field_tracer) = tracer.get_field_tracer_mut(field_idx) else {
                    unreachable!();
                };
                value.serialize(TracerSerializer(field_tracer))
            }
            Self::AsMap(tracer) => value.serialize(TracerSerializer(&mut tracer.value_tracer)),
        }
    }

    fn end(self) -> std::prelude::v1::Result<Self::Ok, Self::Error> {
        match self {
            Self::AsStruct(tracer, _) => tracer.end(),
            Self::AsMap(_) => Ok(()),
        }
    }
}

struct SerializeToString;

#[rustfmt::skip]
impl serde::ser::Serializer for SerializeToString {
    type Ok = String;
    type Error = Error;

    type SerializeSeq = Impossible<Self::Ok, Self::Error>;
    type SerializeTuple = Impossible<Self::Ok, Self::Error>;
    type SerializeTupleVariant = Impossible<Self::Ok, Self::Error>;
    type SerializeMap = Impossible<Self::Ok, Self::Error>;
    type SerializeStruct = Impossible<Self::Ok, Self::Error>;
    type SerializeStructVariant = Impossible<Self::Ok, Self::Error>;
    type SerializeTupleStruct = Impossible<Self::Ok, Self::Error>;

    fn serialize_str(self, val: &str) -> Result<Self::Ok> {
        Ok(val.to_owned())
    }

    unimplemented_fn!(serialize_seq(self, _: Option<usize>) -> Result<Self::SerializeSeq>);
    unimplemented_fn!(serialize_tuple(self, _: usize) -> Result<Self::SerializeTuple>);
    unimplemented_fn!(serialize_tuple_variant(self, _: &'static str, _: u32, _: &'static str, _: usize) -> Result<Self::SerializeTupleVariant>);
    unimplemented_fn!(serialize_bool(self, _: bool) -> Result<Self::Ok>);
    unimplemented_fn!(serialize_i8(self, _: i8) -> Result<Self::Ok>);
    unimplemented_fn!(serialize_i16(self, _: i16) -> Result<Self::Ok>);
    unimplemented_fn!(serialize_i32(self, _: i32) -> Result<Self::Ok>);
    unimplemented_fn!(serialize_i64(self, _: i64) -> Result<Self::Ok>);
    unimplemented_fn!(serialize_u8(self, _: u8) -> Result<Self::Ok>);
    unimplemented_fn!(serialize_u16(self, _: u16) -> Result<Self::Ok>);
    unimplemented_fn!(serialize_u32(self, _: u32) -> Result<Self::Ok>);
    unimplemented_fn!(serialize_u64(self, _: u64) -> Result<Self::Ok>);
    unimplemented_fn!(serialize_f32(self, _: f32) -> Result<Self::Ok>);
    unimplemented_fn!(serialize_f64(self, _: f64) -> Result<Self::Ok>);
    unimplemented_fn!(serialize_char(self, _: char) -> Result<Self::Ok>);
    unimplemented_fn!(serialize_unit(self) -> Result<Self::Ok>);
    unimplemented_fn!(serialize_bytes(self, _: &[u8]) -> Result<Self::Ok>);
    unimplemented_fn!(serialize_none(self) -> Result<Self::Ok>);
    unimplemented_fn!(serialize_map(self, _: Option<usize>) -> Result<Self::SerializeMap>);
    unimplemented_fn!(serialize_struct(self, _: &'static str, _: usize) -> Result<Self::SerializeStruct>);
    unimplemented_fn!(serialize_struct_variant(self, _: &'static str, _: u32, _: &'static str, _: usize) -> Result<Self::SerializeStructVariant>);
    unimplemented_fn!(serialize_tuple_struct(self, _: &'static str, _: usize) -> Result<Self::SerializeTupleStruct>);
    unimplemented_fn!(serialize_unit_struct(self, _: &'static str) -> Result<Self::Ok>);
    unimplemented_fn!(serialize_unit_variant(self, _: &'static str, _: u32, _: &'static str) -> Result<Self::Ok>);
    unimplemented_fn!(serialize_some<T: Serialize + ?Sized>(self, _: &T) -> Result<Self::Ok>);
    unimplemented_fn!(serialize_newtype_struct<T: Serialize + ?Sized>(self, _: &'static str, _: &T) -> Result<Self::Ok>);
    unimplemented_fn!(serialize_newtype_variant<T: Serialize + ?Sized>(self, _: &'static str, _: u32, _: &'static str, _: &T) -> Result<Self::Ok>);
}

#[cfg(test)]
mod test {
    use serde::Serialize;
    use serde_json::{json, Value};

    use crate::internal::schema::{GenericField, TracingOptions};

    use super::*;

    fn test_to_tracer<T: Serialize + ?Sized>(items: &T, options: TracingOptions, expected: Value) {
        let tracer = to_tracer(items, options).unwrap();
        let field = tracer.to_field("$").unwrap();
        let expected = serde_json::from_value::<GenericField>(expected).unwrap();

        assert_eq!(field, expected);
    }

    #[test]
    fn example_i64() {
        test_to_tracer(
            &[13_i64, 21, 42],
            TracingOptions::default(),
            json!({"name": "$", "data_type": "I64"}),
        )
    }

    #[test]
    fn example_i32_nullable_some() {
        let expected = json!({"name": "$", "data_type": "I32", "nullable": true});
        test_to_tracer(&[Some(42_i32)], TracingOptions::default(), expected.clone());
        test_to_tracer(&[None, Some(42_i32)], TracingOptions::default(), expected);
    }

    #[test]
    fn example_simple_struct() {
        #[derive(Serialize)]
        struct S {
            a: u32,
            b: bool,
        }

        let expected = json!({
            "name": "$",
            "data_type": "Struct",
            "children": [
                {"name": "a", "data_type": "U32"},
                {"name": "b", "data_type": "Bool"},
            ],
        });

        test_to_tracer(
            &[S { a: 1, b: false }, S { a: 1, b: true }],
            TracingOptions::default(),
            expected,
        );
    }

    #[test]
    fn example_vec_f32() {
        let expected = json!({
            "name": "$",
            "data_type": "LargeList",
            "children": [
                {"name": "element", "data_type": "F32"},
            ],
        });

        test_to_tracer(
            &[vec![1.0_f32, 2.0_f32], vec![3.0_f32], vec![]],
            TracingOptions::default(),
            expected,
        );
    }

    #[test]
    fn example_vec_nullable_f32() {
        let expected = json!({
            "name": "$",
            "data_type": "LargeList",
            "children": [
                {"name": "element", "data_type": "F32", "nullable": true},
            ],
        });

        test_to_tracer(
            &[vec![Some(1.0_f32), None], vec![Some(3.0_f32)], vec![]],
            TracingOptions::default(),
            expected,
        );
    }

    #[test]
    fn example_tuples() {
        let expected = json!({
            "name": "$",
            "data_type": "Struct",
            "strategy": "TupleAsStruct",
            "children": [
                {"name": "0", "data_type": "F64"},
                {"name": "1", "data_type": "LargeUtf8"},
            ],
        });

        test_to_tracer(
            &[(2.0_f64, "hello world")],
            TracingOptions::default(),
            expected,
        );
    }
}
