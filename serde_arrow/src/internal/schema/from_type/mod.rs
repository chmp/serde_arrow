//! Support for SchemaLike::from_type
#[cfg(test)]
mod test_error_messages;

use serde::{
    de::{DeserializeSeed, Visitor},
    Deserialize, Deserializer,
};

use crate::internal::error::{fail, Error, Result};

use super::{
    tracer::{StructField, Tracer},
    SerdeArrowSchema, TracingMode, TracingOptions,
};

pub fn schema_from_type<'de, T: Deserialize<'de> + ?Sized>(
    options: TracingOptions,
) -> Result<SerdeArrowSchema> {
    let options = options.tracing_mode(TracingMode::FromType);

    let mut tracer = Tracer::new(String::from("$"), options);
    tracer.trace_type::<T>()?;
    tracer.to_schema()
}

impl Tracer {
    pub fn trace_type<'de, T: Deserialize<'de>>(&mut self) -> Result<()> {
        self.reset()?;

        let mut budget = self.get_options().from_type_budget;
        while !self.is_complete() {
            if budget == 0 {
                fail!(
                    concat!(
                        "Could not determine schema from the type after {budget} iterations. ",
                        "Consider increasing the budget option or using `from_samples`.",
                    ),
                    budget = self.get_options().from_type_budget,
                );
            }
            T::deserialize(TraceAny(&mut *self))?;
            budget -= 1;
        }

        self.finish()?;
        Ok(())
    }
}

struct TraceAny<'a>(&'a mut Tracer);

impl<'de, 'a> serde::de::Deserializer<'de> for TraceAny<'a> {
    type Error = Error;

    fn deserialize_any<V: Visitor<'de>>(self, _visitor: V) -> Result<V::Value> {
        fail!(concat!(
            "Non self describing types cannot be traced with `from_type`. ",
            "Consider using `from_samples`. ",
            "One example is `serde_json::Value`. ",
            "the schema depends on the JSON content and cannot be determined from the type alone."
        ));
    }

    fn deserialize_bool<V: Visitor<'de>>(self, visitor: V) -> Result<V::Value> {
        self.0.ensure_bool()?;
        visitor.visit_bool(Default::default())
    }

    fn deserialize_i8<V: Visitor<'de>>(self, visitor: V) -> Result<V::Value> {
        self.0.ensure_i8()?;
        visitor.visit_i8(Default::default())
    }

    fn deserialize_i16<V: Visitor<'de>>(self, visitor: V) -> Result<V::Value> {
        self.0.ensure_i16()?;
        visitor.visit_i16(Default::default())
    }

    fn deserialize_i32<V: Visitor<'de>>(self, visitor: V) -> Result<V::Value> {
        self.0.ensure_i32()?;
        visitor.visit_i32(Default::default())
    }

    fn deserialize_i64<V: Visitor<'de>>(self, visitor: V) -> Result<V::Value> {
        self.0.ensure_i64()?;
        visitor.visit_i64(Default::default())
    }

    fn deserialize_u8<V: Visitor<'de>>(self, visitor: V) -> Result<V::Value> {
        self.0.ensure_u8()?;
        visitor.visit_u8(Default::default())
    }

    fn deserialize_u16<V: Visitor<'de>>(self, visitor: V) -> Result<V::Value> {
        self.0.ensure_u16()?;
        visitor.visit_u16(Default::default())
    }

    fn deserialize_u32<V: Visitor<'de>>(self, visitor: V) -> Result<V::Value> {
        self.0.ensure_u32()?;
        visitor.visit_u32(Default::default())
    }

    fn deserialize_u64<V: Visitor<'de>>(self, visitor: V) -> Result<V::Value> {
        self.0.ensure_u64()?;
        visitor.visit_u64(Default::default())
    }

    fn deserialize_f32<V: Visitor<'de>>(self, visitor: V) -> Result<V::Value> {
        self.0.ensure_f32()?;
        visitor.visit_f32(Default::default())
    }

    fn deserialize_f64<V: Visitor<'de>>(self, visitor: V) -> Result<V::Value> {
        self.0.ensure_f64()?;
        visitor.visit_f64(Default::default())
    }

    fn deserialize_char<V: Visitor<'de>>(self, visitor: V) -> Result<V::Value> {
        self.0.ensure_u32()?;
        visitor.visit_char(Default::default())
    }

    fn deserialize_str<V: Visitor<'de>>(self, visitor: V) -> Result<V::Value> {
        self.0.ensure_utf8()?;
        visitor.visit_borrowed_str("")
    }

    fn deserialize_string<V: Visitor<'de>>(self, visitor: V) -> Result<V::Value> {
        self.0.ensure_utf8()?;
        visitor.visit_string(Default::default())
    }

    fn deserialize_bytes<V: Visitor<'de>>(self, _visitor: V) -> Result<V::Value> {
        fail!("Cannot trace bytes")
    }

    fn deserialize_byte_buf<V: Visitor<'de>>(self, _visitor: V) -> Result<V::Value> {
        fail!("Cannot trace byte bufs")
    }

    fn deserialize_option<V: Visitor<'de>>(self, visitor: V) -> Result<V::Value> {
        self.0.mark_nullable();
        visitor.visit_some(self)
    }

    fn deserialize_unit<V: Visitor<'de>>(self, visitor: V) -> Result<V::Value> {
        self.0.ensure_null()?;
        visitor.visit_unit()
    }

    fn deserialize_unit_struct<V: Visitor<'de>>(
        self,
        _name: &'static str,
        visitor: V,
    ) -> Result<V::Value> {
        self.0.ensure_null()?;
        visitor.visit_unit()
    }

    fn deserialize_newtype_struct<V: Visitor<'de>>(
        self,
        _name: &'static str,
        visitor: V,
    ) -> Result<V::Value> {
        visitor.visit_newtype_struct(self)
    }

    fn deserialize_seq<V: Visitor<'de>>(self, visitor: V) -> Result<V::Value> {
        self.0.ensure_list()?;
        let Tracer::List(tracer) = self.0 else {
            unreachable!()
        };

        visitor.visit_seq(TraceSeq(&mut tracer.item_tracer, true))
    }

    fn deserialize_tuple<V: Visitor<'de>>(self, len: usize, visitor: V) -> Result<V::Value> {
        self.0.ensure_tuple(len)?;
        let Tracer::Tuple(tracer) = self.0 else {
            unreachable!();
        };

        visitor.visit_seq(TraceTupleStruct {
            tracers: &mut tracer.field_tracers,
            pos: 0,
        })
    }

    fn deserialize_tuple_struct<V: Visitor<'de>>(
        self,
        _name: &'static str,
        len: usize,
        visitor: V,
    ) -> Result<V::Value> {
        self.deserialize_tuple(len, visitor)
    }

    fn deserialize_map<V: Visitor<'de>>(self, visitor: V) -> Result<V::Value> {
        if self.0.get_options().map_as_struct {
            fail!(concat!(
                "Cannot trace maps as structs with `from_type`. ",
                "The struct fields cannot be known from the type alone.",
                "Consider using `from_samples`. ",
            ));
        }

        self.0.ensure_map()?;
        let Tracer::Map(tracer) = self.0 else {
            unreachable!()
        };
        visitor.visit_map(TraceMap {
            key_tracer: &mut tracer.key_tracer,
            value_tracer: &mut tracer.value_tracer,
            active: true,
        })
    }

    fn deserialize_struct<V: Visitor<'de>>(
        self,
        _name: &'static str,
        fields: &'static [&'static str],
        visitor: V,
    ) -> Result<V::Value> {
        self.0.ensure_struct(fields)?;
        let Tracer::Struct(tracer) = self.0 else {
            unreachable!()
        };

        visitor.visit_map(TraceStruct {
            fields: &mut tracer.fields,
            pos: 0,
            names: fields,
        })
    }

    fn deserialize_enum<V: Visitor<'de>>(
        self,
        _name: &'static str,
        variants: &'static [&'static str],
        visitor: V,
    ) -> Result<V::Value> {
        self.0.ensure_union(variants)?;
        let Tracer::Union(tracer) = self.0 else {
            unreachable!();
        };

        let idx = tracer
            .variants
            .iter()
            .position(|opt| !opt.as_ref().unwrap().tracer.is_complete())
            .unwrap_or_default();
        if idx >= tracer.variants.len() {
            fail!("invalid variant index");
        }

        let Some(variant) = tracer.variants[idx].as_mut() else {
            fail!("invalid state");
        };

        let res = visitor.visit_enum(TraceEnum {
            tracer: &mut variant.tracer,
            pos: idx,
            variant: &variant.name,
        })?;
        Ok(res)
    }

    fn deserialize_identifier<V: Visitor<'de>>(self, visitor: V) -> Result<V::Value> {
        self.deserialize_str(visitor)
    }

    fn deserialize_ignored_any<V: Visitor<'de>>(self, visitor: V) -> Result<V::Value> {
        // TODO: is this correct?
        visitor.visit_unit()
    }
}

struct TraceMap<'a> {
    key_tracer: &'a mut Tracer,
    value_tracer: &'a mut Tracer,
    active: bool,
}

impl<'de, 'a> serde::de::MapAccess<'de> for TraceMap<'a> {
    type Error = Error;

    fn next_key_seed<K: DeserializeSeed<'de>>(&mut self, seed: K) -> Result<Option<K::Value>> {
        if self.active {
            let key = seed.deserialize(TraceAny(self.key_tracer))?;
            Ok(Some(key))
        } else {
            Ok(None)
        }
    }

    fn next_value_seed<V: DeserializeSeed<'de>>(&mut self, seed: V) -> Result<V::Value> {
        self.active = false;
        seed.deserialize(TraceAny(self.value_tracer))
    }
}

struct TraceTupleStruct<'a> {
    tracers: &'a mut [Tracer],
    pos: usize,
}

impl<'de, 'a> serde::de::SeqAccess<'de> for TraceTupleStruct<'a> {
    type Error = Error;

    fn next_element_seed<T: DeserializeSeed<'de>>(&mut self, seed: T) -> Result<Option<T::Value>> {
        if self.pos >= self.tracers.len() {
            return Ok(None);
        }

        let item = seed.deserialize(TraceAny(&mut self.tracers[self.pos]))?;
        self.pos += 1;

        Ok(Some(item))
    }
}

struct TraceStruct<'a> {
    fields: &'a mut [StructField],
    pos: usize,
    names: &'static [&'static str],
}

impl<'de, 'a> serde::de::MapAccess<'de> for TraceStruct<'a> {
    type Error = Error;

    fn next_key_seed<K: DeserializeSeed<'de>>(&mut self, seed: K) -> Result<Option<K::Value>> {
        if self.pos >= self.names.len() {
            return Ok(None);
        }
        let key = seed.deserialize(IdentifierDeserializer {
            idx: self.pos,
            name: self.names[self.pos],
        })?;
        Ok(Some(key))
    }

    fn next_value_seed<V: DeserializeSeed<'de>>(&mut self, seed: V) -> Result<V::Value> {
        let value = seed.deserialize(TraceAny(&mut self.fields[self.pos].tracer))?;
        self.pos += 1;

        Ok(value)
    }
}

struct TraceEnum<'a> {
    tracer: &'a mut Tracer,
    pos: usize,
    variant: &'a str,
}

impl<'de, 'a> serde::de::EnumAccess<'de> for TraceEnum<'a> {
    type Error = Error;
    type Variant = TraceAny<'a>;

    fn variant_seed<V: DeserializeSeed<'de>>(self, seed: V) -> Result<(V::Value, Self::Variant)> {
        let variant = seed.deserialize(IdentifierDeserializer {
            idx: self.pos,
            name: self.variant,
        })?;
        Ok((variant, TraceAny(self.tracer)))
    }
}

impl<'de, 'a> serde::de::VariantAccess<'de> for TraceAny<'a> {
    type Error = Error;

    fn unit_variant(self) -> Result<()> {
        <()>::deserialize(self)
    }

    fn newtype_variant_seed<T: DeserializeSeed<'de>>(self, seed: T) -> Result<T::Value> {
        seed.deserialize(self)
    }

    fn tuple_variant<V: Visitor<'de>>(self, len: usize, visitor: V) -> Result<V::Value> {
        self.deserialize_tuple(len, visitor)
    }

    fn struct_variant<V: Visitor<'de>>(
        self,
        fields: &'static [&'static str],
        visitor: V,
    ) -> Result<V::Value> {
        self.deserialize_struct("", fields, visitor)
    }
}

struct TraceSeq<'a>(&'a mut Tracer, bool);

impl<'de, 'a> serde::de::SeqAccess<'de> for TraceSeq<'a> {
    type Error = Error;

    fn next_element_seed<T: DeserializeSeed<'de>>(&mut self, seed: T) -> Result<Option<T::Value>> {
        if self.1 {
            self.1 = false;
            let item = seed.deserialize(TraceAny(self.0))?;
            Ok(Some(item))
        } else {
            Ok(None)
        }
    }
}

struct IdentifierDeserializer<'a> {
    idx: usize,
    name: &'a str,
}

macro_rules! unimplemented {
    ($lifetime:lifetime, $name:ident $($tt:tt)*) => {
        fn $name<V: Visitor<$lifetime>>(self $($tt)*, _: V) -> Result<V::Value> {
            fail!("{} is not implemented", stringify!($name))
        }
    };
}

impl<'de, 'a> serde::de::Deserializer<'de> for IdentifierDeserializer<'a> {
    type Error = Error;

    fn deserialize_identifier<V: Visitor<'de>>(self, visitor: V) -> Result<V::Value> {
        self.deserialize_str(visitor)
    }

    fn deserialize_any<V: Visitor<'de>>(self, visitor: V) -> Result<V::Value> {
        self.deserialize_str(visitor)
    }

    fn deserialize_str<V: Visitor<'de>>(self, visitor: V) -> Result<V::Value> {
        visitor.visit_str(self.name)
    }

    fn deserialize_string<V: Visitor<'de>>(self, visitor: V) -> Result<V::Value> {
        visitor.visit_string(self.name.to_owned())
    }

    fn deserialize_u64<V: Visitor<'de>>(self, visitor: V) -> Result<V::Value> {
        visitor.visit_u64(u64::try_from(self.idx)?)
    }

    unimplemented!('de, deserialize_bool);
    unimplemented!('de, deserialize_i8);
    unimplemented!('de, deserialize_i16);
    unimplemented!('de, deserialize_i32);
    unimplemented!('de, deserialize_i64);
    unimplemented!('de, deserialize_u8);
    unimplemented!('de, deserialize_u16);
    unimplemented!('de, deserialize_u32);
    unimplemented!('de, deserialize_f32);
    unimplemented!('de, deserialize_f64);
    unimplemented!('de, deserialize_char);
    unimplemented!('de, deserialize_bytes);
    unimplemented!('de, deserialize_byte_buf);
    unimplemented!('de, deserialize_option);
    unimplemented!('de, deserialize_unit);
    unimplemented!('de, deserialize_unit_struct, _: &'static str);
    unimplemented!('de, deserialize_newtype_struct, _: &'static str);
    unimplemented!('de, deserialize_seq);
    unimplemented!('de, deserialize_tuple, _: usize);
    unimplemented!('de, deserialize_tuple_struct, _: &'static str, _: usize);
    unimplemented!('de, deserialize_map);
    unimplemented!('de, deserialize_struct, _: &'static str, _: &'static [&'static str]);
    unimplemented!('de, deserialize_enum, _: &'static str, _: &'static [&'static str]);
    unimplemented!('de, deserialize_ignored_any);
}
