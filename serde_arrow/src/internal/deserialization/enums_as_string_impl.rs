use serde::de::Visitor;

use crate::internal::error::{fail, Error,Result};

pub struct EnumAccess<'de>(pub &'de str);

impl<'a, 'de> serde::de::EnumAccess<'de> for EnumAccess<'a> {
    type Error = Error;
    type Variant = UnitVariant;

    fn variant_seed<V: serde::de::DeserializeSeed<'de>>(self, seed: V) -> Result<(V::Value, Self::Variant), Self::Error> {
        struct SeedDeserializer<'a>(&'a str);

        macro_rules! unimplemented {
            ($lifetime:lifetime, $name:ident $($tt:tt)*) => {
                fn $name<V: Visitor<$lifetime>>(self $($tt)*, _: V) -> Result<V::Value> {
                    fail!("{} is not implemented", stringify!($name))
                }
            };
        }

        impl<'de, 'a> serde::de::Deserializer<'de> for SeedDeserializer<'a> {
            type Error = Error;

            fn deserialize_identifier<V: Visitor<'de>>(self, visitor: V) -> Result<V::Value> {
                self.deserialize_str(visitor)
            }

            fn deserialize_any<V: Visitor<'de>>(self, visitor: V) -> Result<V::Value> {
                self.deserialize_str(visitor)
            }

            fn deserialize_str<V: Visitor<'de>>(self, visitor: V) -> Result<V::Value> {
                visitor.visit_str(self.0)
            }

            fn deserialize_string<V: Visitor<'de>>(self, visitor: V) -> Result<V::Value> {
                visitor.visit_string(self.0.to_owned())
            }

            unimplemented!('de, deserialize_u64);
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

        Ok((seed.deserialize(SeedDeserializer(self.0))?, UnitVariant))
    }
}

pub struct UnitVariant;

impl<'de> serde::de::VariantAccess<'de> for UnitVariant {
    type Error = Error;

    fn newtype_variant_seed<T: serde::de::DeserializeSeed<'de>>(self, _: T) -> Result<T::Value> {
        fail!("cannot deserialize enums with data from strings")
    }

    fn struct_variant<V: Visitor<'de>>(
        self,
        _: &'static [&'static str],
        _: V,
    ) -> Result<V::Value> {
        fail!("cannot deserialize enums with data from strings")
    }

    fn tuple_variant<V: Visitor<'de>>(self, _: usize, _: V) -> Result<V::Value> {
        fail!("cannot deserialize enums with data from strings")
    }

    fn unit_variant(self) -> Result<(), Self::Error> {
        Ok(())
    }
}
