//! Support for `from_samples`
#[cfg(test)]
mod test_error_messages;

use std::{collections::BTreeMap, sync::Arc};

use serde::{ser::Impossible, Serialize};

use crate::internal::{
    arrow::{DataType, TimeUnit},
    chrono,
    error::{fail, try_, Context, ContextSupport, Error, Result},
    schema::{Strategy, TracingMode, TracingOptions},
};

use super::tracer::{
    ListTracer, MapTracer, StructMode, StructTracer, Tracer, TupleTracer, UnionVariant,
};

impl Tracer {
    pub fn from_samples<T: Serialize>(samples: T, options: TracingOptions) -> Result<Self> {
        let options = options.tracing_mode(TracingMode::FromSamples);
        let mut tracer = Tracer::new(String::from("$"), String::from("$"), Arc::new(options));
        samples.serialize(OuterSequenceSerializer(&mut tracer))?;
        tracer.finish()?;
        tracer.check()?;

        Ok(tracer)
    }
}

struct OuterSequenceSerializer<'a>(&'a mut Tracer);

mod impl_outer_sequence_serializer {
    use super::*;

    macro_rules! unimplemented_fn {
        ($ctx:ident ) => {
            fail!(in $ctx, "Cannot trace non-sequences with `from_samples`: consider wrapping the argument in an array")
        };
    }

    impl<'a> Context for OuterSequenceSerializer<'a> {
        fn annotate(&self, annotations: &mut BTreeMap<String, String>) {
            self.0.annotate(annotations)
        }
    }

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

        fn serialize_tuple_variant(
            self,
            _: &'static str,
            _: u32,
            _: &'static str,
            _: usize,
        ) -> Result<Self::SerializeTupleVariant> {
            Ok(self)
        }

        type SerializeMap = Impossible<Self::Ok, Self::Error>;
        type SerializeStruct = Impossible<Self::Ok, Self::Error>;
        type SerializeStructVariant = Impossible<Self::Ok, Self::Error>;
        type SerializeTupleStruct = Impossible<Self::Ok, Self::Error>;

        fn serialize_bool(self, _: bool) -> Result<Self::Ok> {
            unimplemented_fn!(self)
        }

        fn serialize_i8(self, _: i8) -> Result<Self::Ok> {
            unimplemented_fn!(self)
        }

        fn serialize_i16(self, _: i16) -> Result<Self::Ok> {
            unimplemented_fn!(self)
        }

        fn serialize_i32(self, _: i32) -> Result<Self::Ok> {
            unimplemented_fn!(self)
        }

        fn serialize_i64(self, _: i64) -> Result<Self::Ok> {
            unimplemented_fn!(self)
        }

        fn serialize_u8(self, _: u8) -> Result<Self::Ok> {
            unimplemented_fn!(self)
        }

        fn serialize_u16(self, _: u16) -> Result<Self::Ok> {
            unimplemented_fn!(self)
        }

        fn serialize_u32(self, _: u32) -> Result<Self::Ok> {
            unimplemented_fn!(self)
        }

        fn serialize_u64(self, _: u64) -> Result<Self::Ok> {
            unimplemented_fn!(self)
        }

        fn serialize_f32(self, _: f32) -> Result<Self::Ok> {
            unimplemented_fn!(self)
        }

        fn serialize_f64(self, _: f64) -> Result<Self::Ok> {
            unimplemented_fn!(self)
        }

        fn serialize_char(self, _: char) -> Result<Self::Ok> {
            unimplemented_fn!(self)
        }

        fn serialize_unit(self) -> Result<Self::Ok> {
            unimplemented_fn!(self)
        }

        fn serialize_str(self, _: &str) -> Result<Self::Ok> {
            unimplemented_fn!(self)
        }

        fn serialize_bytes(self, _: &[u8]) -> Result<Self::Ok> {
            unimplemented_fn!(self)
        }

        fn serialize_none(self) -> Result<Self::Ok> {
            unimplemented_fn!(self)
        }

        fn serialize_map(self, _: Option<usize>) -> Result<Self::SerializeMap> {
            unimplemented_fn!(self)
        }

        fn serialize_struct(self, _: &'static str, _: usize) -> Result<Self::SerializeStruct> {
            unimplemented_fn!(self)
        }

        fn serialize_struct_variant(
            self,
            _: &'static str,
            _: u32,
            _: &'static str,
            _: usize,
        ) -> Result<Self::SerializeStructVariant> {
            unimplemented_fn!(self)
        }

        fn serialize_tuple_struct(
            self,
            _: &'static str,
            _: usize,
        ) -> Result<Self::SerializeTupleStruct> {
            unimplemented_fn!(self)
        }

        fn serialize_unit_struct(self, _: &'static str) -> Result<Self::Ok> {
            unimplemented_fn!(self)
        }

        fn serialize_unit_variant(
            self,
            _: &'static str,
            _: u32,
            _: &'static str,
        ) -> Result<Self::Ok> {
            unimplemented_fn!(self)
        }

        fn serialize_some<T: Serialize + ?Sized>(self, _: &T) -> Result<Self::Ok> {
            unimplemented_fn!(self)
        }

        fn serialize_newtype_struct<T: Serialize + ?Sized>(
            self,
            _: &'static str,
            _: &T,
        ) -> Result<Self::Ok> {
            unimplemented_fn!(self)
        }

        fn serialize_newtype_variant<T: Serialize + ?Sized>(
            self,
            _: &'static str,
            _: u32,
            _: &'static str,
            _: &T,
        ) -> Result<Self::Ok> {
            unimplemented_fn!(self)
        }
    }
}

impl<'a> serde::ser::SerializeSeq for OuterSequenceSerializer<'a> {
    type Ok = ();
    type Error = Error;

    fn serialize_element<T: Serialize + ?Sized>(&mut self, value: &T) -> Result<()> {
        try_(|| value.serialize(TracerSerializer(&mut *self.0))).ctx(self)
    }

    fn end(self) -> Result<Self::Ok> {
        Ok(())
    }
}

impl<'a> serde::ser::SerializeTuple for OuterSequenceSerializer<'a> {
    type Ok = ();
    type Error = Error;

    fn serialize_element<T: Serialize + ?Sized>(&mut self, value: &T) -> Result<()> {
        try_(|| value.serialize(TracerSerializer(&mut *self.0))).ctx(self)
    }

    fn end(self) -> Result<Self::Ok> {
        Ok(())
    }
}

impl<'a> serde::ser::SerializeTupleVariant for OuterSequenceSerializer<'a> {
    type Ok = ();
    type Error = Error;

    fn serialize_field<T: Serialize + ?Sized>(&mut self, value: &T) -> Result<()> {
        try_(|| value.serialize(TracerSerializer(&mut *self.0))).ctx(self)
    }

    fn end(self) -> Result<Self::Ok> {
        Ok(())
    }
}

struct TracerSerializer<'a>(&'a mut Tracer);

impl<'a> Context for TracerSerializer<'a> {
    fn annotate(&self, annotations: &mut BTreeMap<String, String>) {
        self.0.annotate(annotations)
    }
}

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
    type SerializeStructVariant = StructSerializer<'a>;
    type SerializeTupleVariant = TupleSerializer<'a>;

    fn serialize_bool(self, _: bool) -> Result<Self::Ok> {
        try_(|| self.0.ensure_primitive(DataType::Boolean)).ctx(&self)
    }

    fn serialize_i8(self, _: i8) -> Result<Self::Ok> {
        try_(|| self.0.ensure_number(DataType::Int8)).ctx(&self)
    }

    fn serialize_i16(self, _: i16) -> Result<Self::Ok> {
        try_(|| self.0.ensure_number(DataType::Int16)).ctx(&self)
    }

    fn serialize_i32(self, _: i32) -> Result<Self::Ok> {
        try_(|| self.0.ensure_number(DataType::Int32)).ctx(&self)
    }

    fn serialize_i64(self, _: i64) -> Result<Self::Ok> {
        try_(|| self.0.ensure_number(DataType::Int64)).ctx(&self)
    }

    fn serialize_u8(self, _: u8) -> Result<Self::Ok> {
        try_(|| self.0.ensure_number(DataType::UInt8)).ctx(&self)
    }

    fn serialize_u16(self, _: u16) -> Result<Self::Ok> {
        try_(|| self.0.ensure_number(DataType::UInt16)).ctx(&self)
    }

    fn serialize_u32(self, _: u32) -> Result<Self::Ok> {
        try_(|| self.0.ensure_number(DataType::UInt32)).ctx(&self)
    }

    fn serialize_u64(self, _: u64) -> Result<Self::Ok> {
        try_(|| self.0.ensure_number(DataType::UInt64)).ctx(&self)
    }

    fn serialize_f32(self, _: f32) -> Result<Self::Ok> {
        try_(|| self.0.ensure_number(DataType::Float32)).ctx(&self)
    }

    fn serialize_f64(self, _: f64) -> Result<Self::Ok> {
        try_(|| self.0.ensure_number(DataType::Float64)).ctx(&self)
    }

    fn serialize_char(self, _: char) -> Result<Self::Ok> {
        try_(|| self.0.ensure_primitive(DataType::UInt32)).ctx(&self)
    }

    fn serialize_unit(self) -> Result<Self::Ok> {
        try_(|| self.0.ensure_primitive(DataType::Null)).ctx(&self)
    }

    fn serialize_str(self, s: &str) -> Result<Self::Ok> {
        try_(|| {
            #[allow(clippy::collapsible_else_if)]
            let (ty, st) = if !self.0.get_options().guess_dates {
                (self.0.get_options().string_type(), None)
            } else {
                if chrono::matches_naive_datetime(s) {
                    (DataType::Date64, Some(Strategy::NaiveStrAsDate64))
                } else if chrono::matches_utc_datetime(s) {
                    (DataType::Date64, Some(Strategy::UtcStrAsDate64))
                } else if chrono::matches_naive_time(s) {
                    (DataType::Time64(TimeUnit::Nanosecond), None)
                } else if chrono::matches_naive_date(s) {
                    (DataType::Date32, None)
                } else {
                    (self.0.get_options().string_type(), None)
                }
            };
            self.0.ensure_primitive_with_strategy(ty, st)
        })
        .ctx(&self)
    }

    fn serialize_bytes(self, _: &[u8]) -> Result<Self::Ok> {
        try_(|| self.0.ensure_primitive(DataType::LargeBinary)).ctx(&self)
    }

    fn serialize_none(self) -> Result<Self::Ok> {
        try_(|| {
            self.0.mark_nullable();
            Ok(())
        })
        .ctx(&self)
    }

    fn serialize_some<T: Serialize + ?Sized>(self, value: &T) -> Result<Self::Ok> {
        try_(|| {
            self.0.mark_nullable();
            value.serialize(TracerSerializer(&mut *self.0))
        })
        .ctx(&self)
    }

    fn serialize_unit_struct(self, _: &'static str) -> Result<Self::Ok> {
        try_(|| TracerSerializer(&mut *self.0).serialize_unit()).ctx(&self)
    }

    fn serialize_newtype_struct<T: Serialize + ?Sized>(
        self,
        _: &'static str,
        value: &T,
    ) -> Result<Self::Ok> {
        try_(|| value.serialize(TracerSerializer(&mut *self.0))).ctx(&self)
    }

    fn serialize_map(self, _: Option<usize>) -> Result<Self::SerializeMap> {
        let mut ctx = BTreeMap::new();
        self.annotate(&mut ctx);

        try_(move || {
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
        })
        .ctx(&ctx)
    }

    fn serialize_seq(self, _: Option<usize>) -> Result<Self::SerializeSeq> {
        let mut ctx = BTreeMap::new();
        self.annotate(&mut ctx);

        try_(move || {
            self.0.ensure_list()?;
            let Tracer::List(tracer) = self.0 else {
                unreachable!();
            };
            Ok(ListSerializer(tracer))
        })
        .ctx(&ctx)
    }

    fn serialize_struct(self, _: &'static str, _: usize) -> Result<Self::SerializeStruct> {
        let mut ctx = BTreeMap::new();
        self.annotate(&mut ctx);

        try_(move || {
            self.0.ensure_struct::<&str>(&[], StructMode::Struct)?;
            let Tracer::Struct(tracer) = self.0 else {
                unreachable!();
            };
            Ok(StructSerializer(tracer))
        })
        .ctx(&ctx)
    }

    fn serialize_tuple(self, len: usize) -> Result<Self::SerializeTuple> {
        let mut ctx = BTreeMap::new();
        self.annotate(&mut ctx);

        try_(move || {
            self.0.ensure_tuple(len)?;
            let Tracer::Tuple(tracer) = self.0 else {
                unreachable!();
            };
            Ok(TupleSerializer::new(tracer))
        })
        .ctx(&ctx)
    }

    fn serialize_tuple_struct(
        self,
        _: &'static str,
        len: usize,
    ) -> Result<Self::SerializeTupleStruct> {
        let mut ctx = BTreeMap::new();
        self.annotate(&mut ctx);

        try_(move || {
            self.0.ensure_tuple(len)?;
            let Tracer::Tuple(tracer) = self.0 else {
                unreachable!();
            };
            Ok(TupleSerializer::new(tracer))
        })
        .ctx(&ctx)
    }

    fn serialize_unit_variant(
        self,
        _: &'static str,
        variant_index: u32,
        variant_name: &'static str,
    ) -> Result<Self::Ok> {
        let mut ctx = BTreeMap::new();
        self.annotate(&mut ctx);

        try_(|| {
            let variant = self.ensure_union_variant(variant_name, variant_index)?;
            variant.tracer.ensure_primitive(DataType::Null)
        })
        .ctx(&ctx)
    }

    fn serialize_newtype_variant<T: Serialize + ?Sized>(
        self,
        _: &'static str,
        variant_index: u32,
        variant_name: &'static str,
        value: &T,
    ) -> Result<Self::Ok> {
        let mut ctx = BTreeMap::new();
        self.annotate(&mut ctx);

        try_(|| {
            let variant = self.ensure_union_variant(variant_name, variant_index)?;
            value.serialize(TracerSerializer(&mut variant.tracer))
        })
        .ctx(&ctx)
    }

    fn serialize_struct_variant(
        self,
        _: &'static str,
        variant_index: u32,
        variant_name: &'static str,
        _: usize,
    ) -> Result<Self::SerializeStructVariant> {
        let mut ctx = BTreeMap::new();
        self.annotate(&mut ctx);

        try_(|| {
            let variant = self.ensure_union_variant(variant_name, variant_index)?;
            variant
                .tracer
                .ensure_struct::<&str>(&[], StructMode::Struct)?;
            let Tracer::Struct(tracer) = &mut variant.tracer else {
                unreachable!();
            };
            Ok(StructSerializer(tracer))
        })
        .ctx(&ctx)
    }

    fn serialize_tuple_variant(
        self,
        _: &'static str,
        variant_index: u32,
        variant_name: &'static str,
        len: usize,
    ) -> Result<Self::SerializeTupleVariant> {
        let mut ctx = BTreeMap::new();
        self.annotate(&mut ctx);

        try_(|| {
            let variant = self.ensure_union_variant(variant_name, variant_index)?;
            variant.tracer.ensure_tuple(len)?;
            let Tracer::Tuple(tracer) = &mut variant.tracer else {
                unreachable!();
            };
            Ok(TupleSerializer::new(tracer))
        })
        .ctx(&ctx)
    }
}

struct StructSerializer<'a>(&'a mut StructTracer);

impl<'a> Context for StructSerializer<'a> {
    fn annotate(&self, annotations: &mut BTreeMap<String, String>) {
        self.0.annotate(annotations)
    }
}

impl<'a> serde::ser::SerializeStruct for StructSerializer<'a> {
    type Ok = ();
    type Error = Error;

    fn serialize_field<T: Serialize + ?Sized>(
        &mut self,
        key: &'static str,
        value: &T,
    ) -> Result<()> {
        try_(|| {
            let field_idx = self.0.ensure_field(key)?;
            let Some(field_tracer) = self.0.get_field_tracer_mut(field_idx) else {
                unreachable!();
            };
            value.serialize(TracerSerializer(field_tracer))
        })
        .ctx(self)
    }

    fn end(self) -> Result<Self::Ok> {
        try_(|| self.0.end()).ctx(&self)
    }
}

impl<'a> serde::ser::SerializeStructVariant for StructSerializer<'a> {
    type Ok = ();
    type Error = Error;

    fn serialize_field<T: Serialize + ?Sized>(
        &mut self,
        key: &'static str,
        value: &T,
    ) -> Result<()> {
        try_(|| {
            let field_idx = self.0.ensure_field(key)?;
            let Some(field_tracer) = self.0.get_field_tracer_mut(field_idx) else {
                unreachable!();
            };
            value.serialize(TracerSerializer(field_tracer))
        })
        .ctx(self)
    }

    fn end(self) -> Result<Self::Ok> {
        try_(|| self.0.end()).ctx(&self)
    }
}

struct ListSerializer<'a>(&'a mut ListTracer);

impl<'a> Context for ListSerializer<'a> {
    fn annotate(&self, annotations: &mut BTreeMap<String, String>) {
        self.0.annotate(annotations)
    }
}

impl<'a> serde::ser::SerializeSeq for ListSerializer<'a> {
    type Ok = ();
    type Error = Error;

    fn serialize_element<T: Serialize + ?Sized>(&mut self, value: &T) -> Result<()> {
        try_(|| value.serialize(TracerSerializer(&mut self.0.item_tracer))).ctx(self)
    }

    fn end(self) -> Result<Self::Ok> {
        Ok(())
    }
}

struct TupleSerializer<'a>(&'a mut TupleTracer, usize);

impl<'a> Context for TupleSerializer<'a> {
    fn annotate(&self, annotations: &mut BTreeMap<String, String>) {
        self.0.annotate(annotations)
    }
}

impl<'a> TupleSerializer<'a> {
    fn new(tracer: &'a mut TupleTracer) -> Self {
        Self(tracer, 0)
    }
}

impl<'a> serde::ser::SerializeTuple for TupleSerializer<'a> {
    type Ok = ();
    type Error = Error;

    fn serialize_element<T: Serialize + ?Sized>(&mut self, value: &T) -> Result<()> {
        try_(|| {
            let pos = self.1;
            value.serialize(TracerSerializer(self.0.field_tracer(pos)))?;
            self.1 += 1;
            Ok(())
        })
        .ctx(self)
    }

    fn end(self) -> Result<Self::Ok> {
        Ok(())
    }
}

impl<'a> serde::ser::SerializeTupleStruct for TupleSerializer<'a> {
    type Ok = ();
    type Error = Error;

    fn serialize_field<T: Serialize + ?Sized>(&mut self, value: &T) -> Result<()> {
        try_(|| {
            let pos = self.1;
            value.serialize(TracerSerializer(self.0.field_tracer(pos)))?;
            self.1 += 1;
            Ok(())
        })
        .ctx(self)
    }

    fn end(self) -> Result<Self::Ok> {
        Ok(())
    }
}

impl<'a> serde::ser::SerializeTupleVariant for TupleSerializer<'a> {
    type Ok = ();
    type Error = Error;

    fn serialize_field<T: Serialize + ?Sized>(&mut self, value: &T) -> Result<()> where {
        try_(|| {
            let pos = self.1;
            value.serialize(TracerSerializer(self.0.field_tracer(pos)))?;
            self.1 += 1;
            Ok(())
        })
        .ctx(self)
    }

    fn end(self) -> Result<Self::Ok> {
        Ok(())
    }
}

enum MapSerializer<'a> {
    AsStruct(&'a mut StructTracer, Option<String>),
    AsMap(&'a mut MapTracer),
}

impl<'a> Context for MapSerializer<'a> {
    fn annotate(&self, annotations: &mut BTreeMap<String, String>) {
        match self {
            Self::AsStruct(tracer, _) => tracer.annotate(annotations),
            Self::AsMap(tracer) => tracer.annotate(annotations),
        }
    }
}

impl<'a> serde::ser::SerializeMap for MapSerializer<'a> {
    type Ok = ();
    type Error = Error;

    fn serialize_key<T: Serialize + ?Sized>(&mut self, key: &T) -> Result<()> {
        try_(|| match self {
            Self::AsStruct(_, next_key) => {
                *next_key = Some(key.serialize(SerializeToString)?);
                Ok(())
            }
            Self::AsMap(tracer) => key.serialize(TracerSerializer(&mut tracer.key_tracer)),
        })
        .ctx(self)
    }

    fn serialize_value<T: Serialize + ?Sized>(&mut self, value: &T) -> Result<()> {
        try_(|| match self {
            Self::AsStruct(tracer, next_key) => {
                let Some(next_key) = next_key.take() else {
                    fail!("Invalid call to serialization methods: serialize_value called without prior call to serialize_key");
                };
                let field_idx = tracer.ensure_field(&next_key)?;
                let Some(field_tracer) = tracer.get_field_tracer_mut(field_idx) else {
                    unreachable!();
                };
                value.serialize(TracerSerializer(field_tracer))
            }
            Self::AsMap(tracer) => value.serialize(TracerSerializer(&mut tracer.value_tracer)),
        })
        .ctx(self)
    }

    fn end(mut self) -> Result<Self::Ok> {
        try_(|| match &mut self {
            Self::AsStruct(tracer, _) => tracer.end(),
            Self::AsMap(_) => Ok(()),
        })
        .ctx(&self)
    }
}

struct SerializeToString;

mod impl_serialize_to_string {
    use super::*;

    macro_rules! unimplemented_fn {
        ($name:ident $($args:tt)* ) => {
            fn $name $($args)* {
                fail!("Invalid argument: cannot interpret key as string");
            }
        };
    }

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
}

#[cfg(test)]
mod test {
    use serde::Serialize;
    use serde_json::{json, Value};

    use crate::internal::schema::{transmute_field, TracingOptions};

    use super::*;

    fn test_to_tracer<T: Serialize + ?Sized>(items: &T, options: TracingOptions, expected: Value) {
        let tracer = Tracer::from_samples(items, options).unwrap();
        let field = tracer.to_field().unwrap();
        let expected = transmute_field(expected).unwrap();

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

    #[test]
    fn example_enum_as_struct_equal_to_struct_with_nullable_fields() {
        #[derive(Serialize)]
        enum Number {
            Real { value: f32 },
            Complex { i: f32, j: f32 },
        }

        #[derive(Serialize, Default)]
        struct StructNumber {
            real_value: Option<f32>,
            complex_i: Option<f32>,
            complex_j: Option<f32>,
        }

        let enum_items = [
            Number::Real { value: 1.0 },
            Number::Complex { i: 0.5, j: 0.5 },
        ];

        let struct_items = [
            StructNumber {
                real_value: Some(1.0),
                ..Default::default()
            },
            StructNumber {
                complex_i: Some(0.5),
                complex_j: Some(0.5),
                ..Default::default()
            },
        ];

        let opts = TracingOptions::default().enums_with_data_as_structs(true);

        let enum_tracer = Tracer::from_samples(&enum_items, opts).unwrap();
        let struct_tracer = Tracer::from_samples(&struct_items, TracingOptions::default()).unwrap();

        assert_eq!(
            enum_tracer.to_field().unwrap(),
            struct_tracer.to_field().unwrap()
        );
    }
}
