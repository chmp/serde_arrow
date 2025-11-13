/// A wrapper around a static field name that compares using ptr and length
#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord)]
pub struct StaticFieldName(*const u8, usize);

impl StaticFieldName {
    pub fn new(s: &'static str) -> Self {
        Self(s.as_ptr(), s.len())
    }
}

macro_rules! unsupported {
    ($($ident:ident),* $(,)?) => {
        $( unsupported!(impl $ident); )*
    };
    (impl serialize_bool) => {
        fn serialize_bool(self, _: bool) -> ::std::result::Result<Self::Ok, Self::Error> {
            Err(Self::Error::custom("serialize_bool is not supported".into()))
        }
    };
    (impl serialize_i8) => {
        fn serialize_i8(self, _: i8) -> ::std::result::Result<Self::Ok, Self::Error> {
            Err(Self::Error::custom("serialize_i8 is not supported".into()))
        }
    };
    (impl serialize_i16) => {
        fn serialize_i16(self, _: i16) -> ::std::result::Result<Self::Ok, Self::Error> {
            Err(Self::Error::custom("serialize_i16 is not supported".into()))
        }
    };
    (impl serialize_i32) => {
        fn serialize_i32(self, _: i32) -> ::std::result::Result<Self::Ok, Self::Error> {
            Err(Self::Error::custom("serialize_i32 is not supported".into()))
        }
    };
    (impl serialize_i64) => {
        fn serialize_i64(self, _: i64) -> ::std::result::Result<Self::Ok, Self::Error> {
            Err(Self::Error::custom("serialize_i64 is not supported".into()))
        }
    };
    (impl serialize_u8) => {
        fn serialize_u8(self, _: u8) -> ::std::result::Result<Self::Ok, Self::Error> {
            Err(Self::Error::custom("serialize_u8 is not supported".into()))
        }
    };
    (impl serialize_u16) => {
        fn serialize_u16(self, _: u16) -> ::std::result::Result<Self::Ok, Self::Error> {
            Err(Self::Error::custom("serialize_u16 is not supported".into()))
        }
    };
    (impl serialize_u32) => {
        fn serialize_u32(self, _: u32) -> ::std::result::Result<Self::Ok, Self::Error> {
            Err(Self::Error::custom("serialize_u32 is not supported".into()))
        }
    };
    (impl serialize_u64) => {
        fn serialize_u64(self, _: u64) -> ::std::result::Result<Self::Ok, Self::Error> {
            Err(Self::Error::custom("serialize_u64 is not supported".into()))
        }
    };
    (impl serialize_f32) => {
        fn serialize_f32(self, _: f32) -> ::std::result::Result<Self::Ok, Self::Error> {
            Err(Self::Error::custom("serialize_f32 is not supported".into()))
        }
    };
    (impl serialize_f64) => {
        fn serialize_f64(self, _: f64) -> ::std::result::Result<Self::Ok, Self::Error> {
            Err(Self::Error::custom("serialize_f64 is not supported".into()))
        }
    };
    (impl serialize_char) => {
        fn serialize_char(self, _: char) -> ::std::result::Result<Self::Ok, Self::Error> {
            todo!()
        }
    };
    (impl serialize_str) => {
        fn serialize_str(self, _: &str) -> ::std::result::Result<Self::Ok, Self::Error> {
            todo!()
        }
    };
    (impl serialize_bytes) => {
        fn serialize_bytes(self, _: &[u8]) -> ::std::result::Result<Self::Ok, Self::Error> {
            todo!()
        }
    };
    (impl serialize_none) => {
        fn serialize_none(self) -> ::std::result::Result<Self::Ok, Self::Error> {
            todo!()
        }
    };
    (impl serialize_some) => {
        fn serialize_some<V: Serialize + ?Sized>(self, _: &V) -> ::std::result::Result<Self::Ok, Self::Error> {
            todo!()
        }

    };
    (impl serialize_unit) => {
        fn serialize_unit(self) -> ::std::result::Result<Self::Ok, Self::Error> {
            todo!()
        }
    };
    (impl serialize_unit_struct) => {
        fn serialize_unit_struct(self, _: &'static str) -> ::std::result::Result<Self::Ok, Self::Error> {
            todo!()
        }
    };
    (impl serialize_unit_variant) => {
        fn serialize_unit_variant(
            self,
            _: &'static str,
            _: u32,
            _: &'static str,
        ) -> ::std::result::Result<Self::Ok, Self::Error> {
            todo!()
        }
    };
    (impl serialize_struct) => {
        type SerializeStruct = ::serde::ser::Impossible<Self::Ok, Self::Error>;

        fn serialize_struct(
            self,
            _: &'static str,
            _: usize,
        ) -> ::std::result::Result<Self::SerializeStruct, Self::Error> {
            todo!()
        }
    };
    (impl serialize_newtype_struct) => {
        fn serialize_newtype_struct<V: Serialize + ?Sized>(
            self,
            _: &'static str,
            _: &V,
        ) -> ::std::result::Result<Self::Ok, Self::Error> {
            todo!()
        }
    };
    (impl serialize_newtype_variant) => {
        fn serialize_newtype_variant<V: Serialize + ?Sized>(
            self,
            _: &'static str,
            _: u32,
            _: &'static str,
            _: &V,
        ) -> ::std::result::Result<Self::Ok, Self::Error> {
            todo!()
        }
    };
    (impl serialize_seq) => {
        type SerializeSeq = ::serde::ser::Impossible<Self::Ok, Self::Error>;

        fn serialize_seq(self, _: Option<usize>) -> ::std::result::Result<Self::SerializeSeq, Self::Error> {
            todo!()
        }
    };
    (impl serialize_tuple) => {
        type SerializeTuple = ::serde::ser::Impossible<Self::Ok, Self::Error>;

        fn serialize_tuple(self, _: usize) -> ::std::result::Result<Self::SerializeTuple, Self::Error> {
            todo!()
        }
    };
    (impl serialize_map) => {
        type SerializeMap = ::serde::ser::Impossible<Self::Ok, Self::Error>;

        fn serialize_map(self, _: Option<usize>) -> ::std::result::Result<Self::SerializeMap, Self::Error> {
            todo!()
        }
    };
    (impl serialize_tuple_struct) => {
        type SerializeTupleStruct = ::serde::ser::Impossible<Self::Ok, Self::Error>;

        fn serialize_tuple_struct(
            self,
            _: &'static str,
            _: usize,
        ) -> ::std::result::Result<Self::SerializeTupleStruct, Self::Error> {
            todo!()
        }
    };
    (impl serialize_tuple_variant) => {
        type SerializeTupleVariant = ::serde::ser::Impossible<Self::Ok, Self::Error>;

        fn serialize_tuple_variant(
            self,
            _: &'static str,
            _: u32,
            _: &'static str,
            _: usize,
        ) -> ::std::result::Result<Self::SerializeTupleVariant, Self::Error> {
            todo!()
        }
    };
    (impl serialize_struct_variant) => {
        type SerializeStructVariant = ::serde::ser::Impossible<Self::Ok, Self::Error>;

        fn serialize_struct_variant(
            self,
            _: &'static str,
            _: u32,
            _: &'static str,
            _: usize,
        ) -> ::std::result::Result<Self::SerializeStructVariant, Self::Error> {
            todo!()
        }
    };
}

pub(crate) use unsupported;
