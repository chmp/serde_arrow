//! Simplify implementing a serde serializer
//!
//! Design decisions:
//!
//! - `serialize_unit` forwards to `serialize_none` if not overwritten
//! - `serialize_some` forwads to the serializer itself
//! - `serialize_tuple_struct` forwards to `serialize_tuple`
//! - `serialize_newtype_struct` forwards to the serializer itself
//! - `SerializeTupleStruct` and `SerializeTupleVariant` are mapped to `SerializeTuple`
//! - `SerializeStructVariant` is mapped to `SerializeStruct`

use crate::internal::error::{Error, Result};

use marrow::array::{BytesArray, BytesViewArray};

/// Helper to define a no-match macro
macro_rules! define_impl_no_match {
    (
        // workaround for: https://github.com/rust-lang/rust/issues/35853
        $d:tt;
        $($known_name:ident),*
        $(,)?
    ) => {
        macro_rules! impl_no_match {
            (
                $d needle:ident, [$d ($d haystack:ident),*], $d item:item
            ) => {
                // check for invalid usage
                $crate::internal::serialization::utils::impl_no_match!(
                    @find,
                    $d needle,
                    [$($known_name),*],
                    compile_error!(concat!("Unknown name: ", stringify!($d needle)));
                );
                $d (
                    $crate::internal::serialization::utils::impl_no_match!(
                        @find,
                        $d haystack,
                        [$($known_name),*],
                        compile_error!(concat!("Unknown name: ", stringify!($d haystack)));
                    );
                )*

                // execute the functionality
                $crate::internal::serialization::utils::impl_no_match!(@find, $d needle, [$d ($d haystack ),*], $d item);
            };
            // match with known function cases
            $(
                (
                    @find,
                    $known_name,
                    [$known_name $d(, $d ident:ident)*],
                    $d item:item
                ) => {};
            )*

            // no match cases
            (
                @find,
                $d needle:ident,
                [$d head:ident $d(, $d tail:ident)* ],
                $d item:item
            ) => {
                $crate::internal::serialization::utils::impl_no_match!(@find, $d needle, [$d($d tail),*], $d item);
            };
            (
                @find,
                $d needle:ident,
                [],
                $d item:item
            ) => {
                $d item
            };
        }

    };
}

define_impl_no_match!(
    $;
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
    serialize_seq,
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
    SerializeStruct,
    SerializeStructVariant,
    SerializeTupleVariant,
    SerializeTupleStruct,
    SerializeTuple,
    SerializeSeq,
    SerializeMap,
);

pub(crate) use impl_no_match;

macro_rules! impl_serializer {
    (
        $lifetime:lifetime, $name:ident;
        $(override $override:ident),*
        $(,)?
    ) => {
        type Ok = ();
        type Error = $crate::internal::error::Error;

        $crate::internal::serialization::utils::impl_no_match!(
            SerializeStruct, [$($override),*],
            type SerializeStruct = $crate::internal::serialization::utils::SerializeStruct<$lifetime>;
        );
        $crate::internal::serialization::utils::impl_no_match!(
            SerializeStructVariant, [$($override),*],
            type SerializeStructVariant = $crate::internal::serialization::utils::SerializeStruct<$lifetime>;
        );
        $crate::internal::serialization::utils::impl_no_match!(
            SerializeTupleVariant, [$($override),*],
            type SerializeTupleVariant = $crate::internal::serialization::utils::SerializeTuple<$lifetime>;
        );
        $crate::internal::serialization::utils::impl_no_match!(
            SerializeTupleStruct, [$($override),*],
            type SerializeTupleStruct = $crate::internal::serialization::utils::SerializeTuple<$lifetime>;
        );
        $crate::internal::serialization::utils::impl_no_match!(
            SerializeTuple, [$($override),*],
            type SerializeTuple = $crate::internal::serialization::utils::SerializeTuple<$lifetime>;
        );
        $crate::internal::serialization::utils::impl_no_match!(
            SerializeSeq, [$($override),*],
            type SerializeSeq = $crate::internal::serialization::utils::SerializeSeq<$lifetime>;
        );
        $crate::internal::serialization::utils::impl_no_match!(
            SerializeMap, [$($override),*],
            type SerializeMap = $crate::internal::serialization::utils::SerializeMap<$lifetime>;
        );
        $crate::internal::serialization::utils::impl_no_match!(
            serialize_some, [$($override),*],
            fn serialize_some<V: ::serde::Serialize + ?::std::marker::Sized>(self, v: &V) -> ::std::result::Result<Self::Ok, Self::Error> {
                v.serialize(self)
            }
        );
        $crate::internal::serialization::utils::impl_no_match!(
            serialize_unit, [$($override),*],
            fn serialize_unit(self) -> ::std::result::Result<Self::Ok, Self::Error> {
                ::serde::Serializer::serialize_none(self)
            }
        );
        $crate::internal::serialization::utils::impl_no_match!(
            serialize_unit_struct, [$($override),*],
            fn serialize_unit_struct(self, _: &'static str) -> ::std::result::Result<Self::Ok, Self::Error> {
                ::serde::ser::Serializer::serialize_unit(self)
            }
        );
        $crate::internal::serialization::utils::impl_no_match!(
            serialize_unit_variant, [$($override),*],
            fn serialize_unit_variant(self, _: &'static str, _: u32, _: &'static str) -> ::std::result::Result<Self::Ok, Self::Error> {
                ::serde::ser::Serializer::serialize_unit(self)
            }
        );
        // handle newtype structs and newtype variants per default as transparent wrappers
        $crate::internal::serialization::utils::impl_no_match!(
            serialize_newtype_struct, [$($override),*],
            fn serialize_newtype_struct<V: ::serde::Serialize + ?::std::marker::Sized>(
                self,
                _: &'static str,
                value: &V
            )  -> ::std::result::Result<Self::Ok, Self::Error>{
                value.serialize(self)
            }
        );
        $crate::internal::serialization::utils::impl_no_match!(
            serialize_newtype_variant, [$($override),*],
            fn serialize_newtype_variant<V: ::serde::Serialize + ?::std::marker::Sized>(
                self,
                _: &'static str,
                _: u32,
                _: &'static str,
                value: &V,
            ) -> ::std::result::Result<Self::Ok, Self::Error> {
                value.serialize(self)
            }

        );
        // handle tuple structs and tuple variants just like tuples
        $crate::internal::serialization::utils::impl_no_match!(
            serialize_tuple_struct, [$($override),*],
            fn serialize_tuple_struct(
                self,
                _: &'static str,
                len: ::std::primitive::usize,
            ) -> ::std::result::Result<Self::SerializeTupleStruct, Self::Error> {
                ::serde::Serializer::serialize_tuple(self, len)
            }
        );
        $crate::internal::serialization::utils::impl_no_match!(
            serialize_tuple_variant, [$($override),*],
            fn serialize_tuple_variant(
                self,
                _: &'static str,
                _: u32,
                _: &'static str,
                len: usize,
            ) -> ::std::result::Result<Self::SerializeTupleVariant, Self::Error> {
                ::serde::Serializer::serialize_tuple(self, len)
            }
        );
        $crate::internal::serialization::utils::impl_no_match!(
            serialize_struct_variant, [$($override),*],
            fn serialize_struct_variant(
                self,
                _: &'static str,
                _: u32,
                variant_name: &'static str,
                len: usize,
            ) -> ::std::result::Result<Self::SerializeStructVariant, Self::Error> {
                ::serde::ser::Serializer::serialize_struct(self, variant_name, len)
            }
        );
        $crate::internal::serialization::utils::impl_no_match!(
            serialize_bool, [$($override),*],
            fn serialize_bool(self, _: bool) -> ::std::result::Result<Self::Ok, Self::Error> {
                $crate::internal::error::fail!("{} does not support serialize_bool", stringify!($name));
            }
        );
        $crate::internal::serialization::utils::impl_no_match!(
            serialize_i8, [$($override),*],
            fn serialize_i8(self, _: i8) -> ::std::result::Result<Self::Ok, Self::Error> {
                $crate::internal::error::fail!("{} does not support serialize_i8", stringify!($name));
            }
        );
        $crate::internal::serialization::utils::impl_no_match!(
            serialize_i16, [$($override),*],
            fn serialize_i16(self, _: i16) -> ::std::result::Result<Self::Ok, Self::Error> {
                 $crate::internal::error::fail!("{} does not support serialize_i16", stringify!($name));
            }
        );
        $crate::internal::serialization::utils::impl_no_match!(
            serialize_i32, [$($override),*],
            fn serialize_i32(self, _: i32) -> ::std::result::Result<Self::Ok, Self::Error> {
                $crate::internal::error::fail!("{} does not support serialize_i32", stringify!($name));
            }
        );
        $crate::internal::serialization::utils::impl_no_match!(
            serialize_i64, [$($override),*],
            fn serialize_i64(self, _: i64) -> ::std::result::Result<Self::Ok, Self::Error> {
                $crate::internal::error::fail!("{} does not support serialize_i64", stringify!($name));
            }
        );
        $crate::internal::serialization::utils::impl_no_match!(
            serialize_u8, [$($override),*],
            fn serialize_u8(self, _: u8) -> ::std::result::Result<Self::Ok, Self::Error> {
                $crate::internal::error::fail!("{} does not support serialize_u8", stringify!($name));
            }
        );
        $crate::internal::serialization::utils::impl_no_match!(
            serialize_u16, [$($override),*],
            fn serialize_u16(self, _: u16) -> ::std::result::Result<Self::Ok, Self::Error> {
                $crate::internal::error::fail!("{} does not support serialize_u16", stringify!($name));
            }
        );
        $crate::internal::serialization::utils::impl_no_match!(
            serialize_u32, [$($override),*],
            fn serialize_u32(self, _: u32) -> ::std::result::Result<Self::Ok, Self::Error> {
                $crate::internal::error::fail!("{} does not support serialize_u32", stringify!($name));
            }
        );
        $crate::internal::serialization::utils::impl_no_match!(
            serialize_u64, [$($override),*],
            fn serialize_u64(self, _: u64) -> ::std::result::Result<Self::Ok, Self::Error> {
                $crate::internal::error::fail!("{} does not support serialize_u64", stringify!($name));
            }
        );
        $crate::internal::serialization::utils::impl_no_match!(
            serialize_f32, [$($override),*],
            fn serialize_f32(self, _: f32) -> ::std::result::Result<Self::Ok, Self::Error> {
                $crate::internal::error::fail!("{} does not support serialize_f32", stringify!($name));
            }
        );
        $crate::internal::serialization::utils::impl_no_match!(
            serialize_f64, [$($override),*],
            fn serialize_f64(self, _: f64) -> ::std::result::Result<Self::Ok, Self::Error> {
                $crate::internal::error::fail!("{} does not support serialize_f64", stringify!($name));
            }
        );
        $crate::internal::serialization::utils::impl_no_match!(
            serialize_char, [$($override),*],
            fn serialize_char(self, _: char) -> ::std::result::Result<Self::Ok, Self::Error> {
                $crate::internal::error::fail!("{} does not support serialize_char", stringify!($name));
            }
        );
        $crate::internal::serialization::utils::impl_no_match!(
            serialize_str, [$($override),*],
            fn serialize_str(self, _: &str) -> ::std::result::Result<Self::Ok, Self::Error> {
                $crate::internal::error::fail!("{} does not support serialize_str", stringify!($name));
            }
        );
        $crate::internal::serialization::utils::impl_no_match!(
            serialize_bytes, [$($override),*],
            fn serialize_bytes(self, _: &[u8]) -> ::std::result::Result<Self::Ok, Self::Error> {
                $crate::internal::error::fail!("{} does not support serialize_bytes", stringify!($name));
            }
        );
        $crate::internal::serialization::utils::impl_no_match!(
            serialize_none, [$($override),*],
            fn serialize_none(self) -> ::std::result::Result<Self::Ok, Self::Error> {
                $crate::internal::error::fail!("{} does not support serialize_none", stringify!($name));
            }
        );
        $crate::internal::serialization::utils::impl_no_match!(
            serialize_struct, [$($override),*],
            fn serialize_struct(
                self,
                _: &'static str,
                _: usize,
            ) -> ::std::result::Result<Self::SerializeStruct, Self::Error> {
                $crate::internal::error::fail!("{} does not support serialize_struct", stringify!($name));
            }
        );
        $crate::internal::serialization::utils::impl_no_match!(
            serialize_seq, [$($override),*],
            fn serialize_seq(
                self,
                _: ::std::option::Option<::std::primitive::usize>
            ) -> ::std::result::Result<Self::SerializeSeq, Self::Error> {
                $crate::internal::error::fail!("{} does not support serialize_seq", stringify!($name));
            }
        );
        $crate::internal::serialization::utils::impl_no_match!(
            serialize_tuple, [$($override),*],
            fn serialize_tuple(
                self,
                _: ::std::primitive::usize,
            ) -> ::std::result::Result<Self::SerializeTuple, Self::Error> {
                $crate::internal::error::fail!("{} does not support serialize_tuple", stringify!($name));
            }
        );
        $crate::internal::serialization::utils::impl_no_match!(
            serialize_map, [$($override),*],
            fn serialize_map(
                self,
                _: ::std::option::Option<::std::primitive::usize>,
            ) -> ::std::result::Result<Self::SerializeMap, Self::Error> {
                $crate::internal::error::fail!("{} does not support serialize_map", stringify!($name));
            }
        );
    };
}

pub(crate) use impl_serializer;

macro_rules! define_serializer_wrapper {
    ($name:ident {
        dispatch $dispatcher:ident,
        $(
            $variant:ident($ty:ty),
        )*
    }) => {
        pub enum $name<'a> {
            $($variant(&'a mut $ty),)*
        }

        macro_rules! $dispatcher {
            ($obj:expr, $var:ident => $expr:expr) => {
                match $obj {
                    $(
                        $name::$variant($var) => $expr,
                    )*
                }
            };
        }
    };
}

define_serializer_wrapper!(SerializeStruct {
    dispatch dispatch_serialize_struct,
    Struct(super::struct_builder::StructBuilder),
});

impl serde::ser::SerializeStruct for SerializeStruct<'_> {
    type Ok = ();
    type Error = Error;

    fn serialize_field<T: ?Sized + serde::Serialize>(
        &mut self,
        key: &'static str,
        value: &T,
    ) -> Result<()> {
        dispatch_serialize_struct!(self, builder => builder.serialize_field(key, value))
    }

    fn end(self) -> std::result::Result<Self::Ok, Self::Error> {
        dispatch_serialize_struct!(self, builder => builder.end())
    }
}

impl serde::ser::SerializeStructVariant for SerializeStruct<'_> {
    type Ok = ();
    type Error = Error;

    fn serialize_field<T: ?Sized + serde::Serialize>(
        &mut self,
        key: &'static str,
        value: &T,
    ) -> Result<()> {
        serde::ser::SerializeStruct::serialize_field(self, key, value)
    }

    fn end(self) -> Result<()> {
        serde::ser::SerializeStruct::end(self)
    }
}

define_serializer_wrapper!(SerializeSeq {
    dispatch dispatch_serialize_seq,
    Binary(super::binary_builder::BinaryBuilder<BytesArray<i32>>),
    LargeBinary(super::binary_builder::BinaryBuilder<BytesArray<i64>>),
    BinaryView(super::binary_builder::BinaryBuilder<BytesViewArray>),
    FixedSizeBinary(super::fixed_size_binary_builder::FixedSizeBinaryBuilder),
    FixedSizeList(super::fixed_size_list_builder::FixedSizeListBuilder),
    List(super::list_builder::ListBuilder<i32>),
    LargeList(super::list_builder::ListBuilder<i64>),
});

impl serde::ser::SerializeSeq for SerializeSeq<'_> {
    type Ok = ();
    type Error = Error;

    fn serialize_element<T: ?Sized + serde::Serialize>(&mut self, value: &T) -> Result<()> {
        dispatch_serialize_seq!(self, builder => serde::ser::SerializeSeq::serialize_element(builder, value))
    }

    fn end(self) -> Result<()> {
        dispatch_serialize_seq!(self, builder => serde::ser::SerializeSeq::end(builder))
    }
}

define_serializer_wrapper!(SerializeMap {
    dispatch dispatch_serialize_map,
    Map(super::map_builder::MapBuilder),
    Struct(super::struct_builder::StructBuilder),
});

impl serde::ser::SerializeMap for SerializeMap<'_> {
    type Ok = ();
    type Error = Error;

    fn serialize_key<T: ?Sized + serde::Serialize>(&mut self, key: &T) -> Result<()> {
        dispatch_serialize_map!(self, builder => serde::ser::SerializeMap::serialize_key(builder, key))
    }

    fn serialize_value<T: ?Sized + serde::Serialize>(&mut self, value: &T) -> Result<()> {
        dispatch_serialize_map!(self, builder => serde::ser::SerializeMap::serialize_value(builder, value))
    }

    fn end(self) -> Result<()> {
        dispatch_serialize_map!(self, builder => serde::ser::SerializeMap::end(builder))
    }
}

define_serializer_wrapper!(SerializeTuple {
    dispatch dispatch_serialize_tuple,
    Struct(super::struct_builder::StructBuilder),
    Binary(super::binary_builder::BinaryBuilder<BytesArray<i32>>),
    LargeBinary(super::binary_builder::BinaryBuilder<BytesArray<i64>>),
    BinaryView(super::binary_builder::BinaryBuilder<BytesViewArray>),
    FixedSizeBinary(super::fixed_size_binary_builder::FixedSizeBinaryBuilder),
    List(super::list_builder::ListBuilder<i32>),
    LargeList(super::list_builder::ListBuilder<i64>),
    FixedSizeList(super::fixed_size_list_builder::FixedSizeListBuilder),
});

impl serde::ser::SerializeTuple for SerializeTuple<'_> {
    type Ok = ();
    type Error = Error;

    fn serialize_element<T: ?Sized + serde::Serialize>(&mut self, value: &T) -> Result<()> {
        dispatch_serialize_tuple!(self, builder => serde::ser::SerializeTuple::serialize_element(builder, value))
    }

    fn end(self) -> std::result::Result<Self::Ok, Self::Error> {
        dispatch_serialize_tuple!(self, builder => serde::ser::SerializeTuple::end(builder))
    }
}

impl serde::ser::SerializeTupleStruct for SerializeTuple<'_> {
    type Ok = ();
    type Error = Error;

    fn serialize_field<T: ?Sized + serde::Serialize>(&mut self, value: &T) -> Result<()> {
        serde::ser::SerializeTuple::serialize_element(self, value)
    }

    fn end(self) -> Result<()> {
        serde::ser::SerializeTuple::end(self)
    }
}

impl serde::ser::SerializeTupleVariant for SerializeTuple<'_> {
    type Ok = ();
    type Error = Error;

    fn serialize_field<T: ?Sized + serde::Serialize>(&mut self, value: &T) -> Result<()> {
        serde::ser::SerializeTuple::serialize_element(self, value)
    }

    fn end(self) -> Result<()> {
        serde::ser::SerializeTuple::end(self)
    }
}
