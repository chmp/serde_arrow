//! Support for SchemaLike::from_type
#[cfg(test)]
mod test_error_messages;

use std::{collections::BTreeMap, sync::Arc};

use serde::{
    de::{DeserializeSeed, Visitor},
    Deserialize, Deserializer,
};

use crate::internal::{
    arrow::DataType,
    error::{fail, try_, Context, ContextSupport, Error, Result},
    schema::{TracingMode, TracingOptions},
};

use super::tracer::{StructField, StructMode, Tracer};

impl Tracer {
    pub fn from_type<'de, T: Deserialize<'de>>(options: TracingOptions) -> Result<Self> {
        let options = options.tracing_mode(TracingMode::FromType);
        let mut tracer = Tracer::new(String::from("$"), String::from("$"), Arc::new(options));

        let mut budget = tracer.get_options().from_type_budget;
        while !tracer.is_complete() {
            if budget == 0 {
                fail!(
                    concat!(
                        "Could not determine schema from the type after {budget} iterations. ",
                        "Consider increasing the budget option or using `from_samples`.",
                    ),
                    budget = tracer.get_options().from_type_budget,
                );
            }
            let res = T::deserialize(TraceAny(&mut tracer));
            if let Err(err) = res {
                if !is_non_self_describing_error(err.message()) {
                    return Err(err);
                }
                let mut err = err;
                err.modify_message(|message| {
                    *message = format!(
                        concat!(
                            "{message}{maybe_period} ",
                            "It seems that `from_type` encountered a non self describing type. ",
                            "Consider using `from_samples` instead. ",
                        ),
                        message = message,
                        maybe_period = if message.trim_end().ends_with('.') {
                            ""
                        } else {
                            "."
                        },
                    );
                });
                return Err(err);
            }

            budget -= 1;
        }

        tracer.finish()?;
        tracer.check()?;

        Ok(tracer)
    }
}

// check for known error messages of non self describing types
fn is_non_self_describing_error(s: &str) -> bool {
    // chrono::*
    s.contains("premature end of input")
        // uuid::Uuid
        || s.contains("UUID parsing failed")
        // std::net::IpAddr
        || s.contains("invalid IP address syntax")
}

struct TraceAny<'a>(&'a mut Tracer);

impl<'a> Context for TraceAny<'a> {
    fn annotate(&self, annotations: &mut BTreeMap<String, String>) {
        self.0.annotate(annotations)
    }
}

impl<'de, 'a> serde::de::Deserializer<'de> for TraceAny<'a> {
    type Error = Error;

    fn deserialize_any<V: Visitor<'de>>(self, _visitor: V) -> Result<V::Value> {
        fail!(
            in self,
            concat!(
            "Non self describing types cannot be traced with `from_type`. ",
            "Consider using `from_samples`. ",
            "One example is `serde_json::Value`: ",
            "the schema depends on the JSON content and cannot be determined from the type alone."
        ));
    }

    fn deserialize_bool<V: Visitor<'de>>(self, visitor: V) -> Result<V::Value> {
        try_(|| {
            self.0.ensure_primitive(DataType::Boolean)?;
            visitor.visit_bool(Default::default())
        })
        .ctx(&self)
    }

    fn deserialize_i8<V: Visitor<'de>>(self, visitor: V) -> Result<V::Value> {
        try_(|| {
            self.0.ensure_primitive(DataType::Int8)?;
            visitor.visit_i8(Default::default())
        })
        .ctx(&self)
    }

    fn deserialize_i16<V: Visitor<'de>>(self, visitor: V) -> Result<V::Value> {
        try_(|| {
            self.0.ensure_primitive(DataType::Int16)?;
            visitor.visit_i16(Default::default())
        })
        .ctx(&self)
    }

    fn deserialize_i32<V: Visitor<'de>>(self, visitor: V) -> Result<V::Value> {
        try_(|| {
            self.0.ensure_primitive(DataType::Int32)?;
            visitor.visit_i32(Default::default())
        })
        .ctx(&self)
    }

    fn deserialize_i64<V: Visitor<'de>>(self, visitor: V) -> Result<V::Value> {
        try_(|| {
            self.0.ensure_primitive(DataType::Int64)?;
            visitor.visit_i64(Default::default())
        })
        .ctx(&self)
    }

    fn deserialize_u8<V: Visitor<'de>>(self, visitor: V) -> Result<V::Value> {
        try_(|| {
            self.0.ensure_primitive(DataType::UInt8)?;
            visitor.visit_u8(Default::default())
        })
        .ctx(&self)
    }

    fn deserialize_u16<V: Visitor<'de>>(self, visitor: V) -> Result<V::Value> {
        try_(|| {
            self.0.ensure_primitive(DataType::UInt16)?;
            visitor.visit_u16(Default::default())
        })
        .ctx(&self)
    }

    fn deserialize_u32<V: Visitor<'de>>(self, visitor: V) -> Result<V::Value> {
        try_(|| {
            self.0.ensure_primitive(DataType::UInt32)?;
            visitor.visit_u32(Default::default())
        })
        .ctx(&self)
    }

    fn deserialize_u64<V: Visitor<'de>>(self, visitor: V) -> Result<V::Value> {
        try_(|| {
            self.0.ensure_primitive(DataType::UInt64)?;
            visitor.visit_u64(Default::default())
        })
        .ctx(&self)
    }

    fn deserialize_f32<V: Visitor<'de>>(self, visitor: V) -> Result<V::Value> {
        try_(|| {
            self.0.ensure_primitive(DataType::Float32)?;
            visitor.visit_f32(Default::default())
        })
        .ctx(&self)
    }

    fn deserialize_f64<V: Visitor<'de>>(self, visitor: V) -> Result<V::Value> {
        try_(|| {
            self.0.ensure_primitive(DataType::Float64)?;
            visitor.visit_f64(Default::default())
        })
        .ctx(&self)
    }

    fn deserialize_char<V: Visitor<'de>>(self, visitor: V) -> Result<V::Value> {
        try_(|| {
            self.0.ensure_primitive(DataType::UInt32)?;
            visitor.visit_char(Default::default())
        })
        .ctx(&self)
    }

    fn deserialize_str<V: Visitor<'de>>(self, visitor: V) -> Result<V::Value> {
        try_(|| {
            self.0.ensure_utf8(DataType::LargeUtf8, None)?;
            visitor.visit_borrowed_str("")
        })
        .ctx(&self)
    }

    fn deserialize_string<V: Visitor<'de>>(self, visitor: V) -> Result<V::Value> {
        try_(|| {
            self.0.ensure_utf8(DataType::LargeUtf8, None)?;
            visitor.visit_string(Default::default())
        })
        .ctx(&self)
    }

    fn deserialize_bytes<V: Visitor<'de>>(self, visitor: V) -> Result<V::Value> {
        try_(|| {
            self.0.ensure_primitive(DataType::LargeBinary)?;
            visitor.visit_borrowed_bytes(&[])
        })
        .ctx(&self)
    }

    fn deserialize_byte_buf<V: Visitor<'de>>(self, visitor: V) -> Result<V::Value> {
        try_(|| {
            self.0.ensure_primitive(DataType::LargeBinary)?;
            visitor.visit_byte_buf(Default::default())
        })
        .ctx(&self)
    }

    fn deserialize_option<V: Visitor<'de>>(self, visitor: V) -> Result<V::Value> {
        try_(|| {
            self.0.mark_nullable();
            visitor.visit_some(TraceAny(&mut *self.0))
        })
        .ctx(&self)
    }

    fn deserialize_unit<V: Visitor<'de>>(self, visitor: V) -> Result<V::Value> {
        try_(|| {
            self.0.ensure_primitive(DataType::Null)?;
            visitor.visit_unit()
        })
        .ctx(&self)
    }

    fn deserialize_unit_struct<V: Visitor<'de>>(
        self,
        _name: &'static str,
        visitor: V,
    ) -> Result<V::Value> {
        try_(|| {
            self.0.ensure_primitive(DataType::Null)?;
            visitor.visit_unit()
        })
        .ctx(&self)
    }

    fn deserialize_newtype_struct<V: Visitor<'de>>(
        self,
        _name: &'static str,
        visitor: V,
    ) -> Result<V::Value> {
        try_(|| visitor.visit_newtype_struct(TraceAny(&mut *self.0))).ctx(&self)
    }

    fn deserialize_seq<V: Visitor<'de>>(self, visitor: V) -> Result<V::Value> {
        try_(|| {
            self.0.ensure_list()?;
            let Tracer::List(tracer) = self.0 else {
                unreachable!()
            };

            visitor.visit_seq(TraceSeq(&mut tracer.item_tracer, true))
        })
        .ctx(&self)
    }

    fn deserialize_tuple<V: Visitor<'de>>(self, len: usize, visitor: V) -> Result<V::Value> {
        try_(|| {
            self.0.ensure_tuple(len)?;
            let Tracer::Tuple(tracer) = self.0 else {
                unreachable!();
            };

            visitor.visit_seq(TraceTupleStruct {
                tracers: &mut tracer.field_tracers,
                pos: 0,
            })
        })
        .ctx(&self)
    }

    fn deserialize_tuple_struct<V: Visitor<'de>>(
        self,
        _name: &'static str,
        len: usize,
        visitor: V,
    ) -> Result<V::Value> {
        try_(|| TraceAny(&mut *self.0).deserialize_tuple(len, visitor)).ctx(&self)
    }

    fn deserialize_map<V: Visitor<'de>>(self, visitor: V) -> Result<V::Value> {
        try_(|| {
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
        })
        .ctx(&self)
    }

    fn deserialize_struct<V: Visitor<'de>>(
        self,
        _name: &'static str,
        fields: &'static [&'static str],
        visitor: V,
    ) -> Result<V::Value> {
        try_(|| {
            self.0.ensure_struct(fields, StructMode::Struct)?;
            let Tracer::Struct(tracer) = self.0 else {
                unreachable!()
            };

            visitor.visit_map(TraceStruct {
                fields: &mut tracer.fields,
                pos: 0,
                names: fields,
            })
        })
        .ctx(&self)
    }

    fn deserialize_enum<V: Visitor<'de>>(
        self,
        _name: &'static str,
        variants: &'static [&'static str],
        visitor: V,
    ) -> Result<V::Value> {
        try_(|| {
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
                fail!("Invalid variant index");
            }

            let Some(variant) = tracer.variants[idx].as_mut() else {
                fail!("Invalid state");
            };

            let res = visitor.visit_enum(TraceEnum {
                tracer: &mut variant.tracer,
                pos: idx,
                variant: &variant.name,
            })?;
            Ok(res)
        })
        .ctx(&self)
    }

    fn deserialize_identifier<V: Visitor<'de>>(self, visitor: V) -> Result<V::Value> {
        try_(|| TraceAny(&mut *self.0).deserialize_str(visitor)).ctx(&self)
    }

    fn deserialize_ignored_any<V: Visitor<'de>>(self, visitor: V) -> Result<V::Value> {
        try_(|| {
            // TODO: is this correct?
            visitor.visit_unit()
        })
        .ctx(&self)
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
