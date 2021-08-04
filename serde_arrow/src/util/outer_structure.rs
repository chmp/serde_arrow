use serde::ser::{Impossible, Serialize, SerializeMap, SerializeSeq, SerializeStruct, Serializer};

use super::string_extractor::StringExtractor;
use crate::{fail, Error, Result};

enum State {
    Start,
    OuterSequence,
    Item,
    Field(String),
    End,
}

pub trait RecordBuilder {
    fn start(&mut self) -> Result<(), Error>;
    fn field<T: Serialize + ?Sized>(&mut self, key: &str, value: &T) -> Result<(), Error>;
    fn end(&mut self) -> Result<(), Error>;
}

pub struct OuterSerializer<T: RecordBuilder> {
    state: State,
    record_serializer: T,
}

impl<T: RecordBuilder> OuterSerializer<T> {
    pub fn new(record_serializer: T) -> Result<Self> {
        let res = OuterSerializer {
            state: State::Start,
            record_serializer,
        };
        Ok(res)
    }

    pub fn into_inner(self) -> T {
        self.record_serializer
    }
}

macro_rules! unsupported {
    ($name:ident, $ty:ty) => {
        fn $name(self, _v: $ty) -> Result<()> {
            return Err(Error::Custom(String::from("Not supported")));
        }
    };
}

impl<'a, S: RecordBuilder> Serializer for &'a mut OuterSerializer<S> {
    type Ok = ();
    type Error = Error;

    type SerializeSeq = Self;
    type SerializeStruct = Self;
    type SerializeTuple = Impossible<Self::Ok, Self::Error>;
    type SerializeTupleStruct = Impossible<Self::Ok, Self::Error>;
    type SerializeTupleVariant = Impossible<Self::Ok, Self::Error>;
    type SerializeMap = Self;
    type SerializeStructVariant = Impossible<Self::Ok, Self::Error>;

    unsupported!(serialize_bool, bool);
    unsupported!(serialize_i8, i8);
    unsupported!(serialize_i16, i16);
    unsupported!(serialize_i32, i32);
    unsupported!(serialize_i64, i64);
    unsupported!(serialize_u8, u8);
    unsupported!(serialize_u16, u16);
    unsupported!(serialize_u32, u32);
    unsupported!(serialize_u64, u64);
    unsupported!(serialize_f32, f32);
    unsupported!(serialize_f64, f64);
    unsupported!(serialize_char, char);
    unsupported!(serialize_str, &str);
    unsupported!(serialize_bytes, &[u8]);

    fn serialize_none(self) -> Result<()> {
        fail!("Not supported");
    }

    fn serialize_some<T>(self, _value: &T) -> Result<()>
    where
        T: ?Sized + Serialize,
    {
        fail!("Not supported");
    }

    fn serialize_unit(self) -> Result<()> {
        fail!("Not supported");
    }

    fn serialize_unit_struct(self, _name: &'static str) -> Result<()> {
        fail!("Not supported");
    }

    fn serialize_unit_variant(
        self,
        _name: &'static str,
        _variant_index: u32,
        _variant: &'static str,
    ) -> Result<()> {
        fail!("Not supported");
    }

    fn serialize_newtype_struct<T>(self, _name: &'static str, _value: &T) -> Result<()>
    where
        T: ?Sized + Serialize,
    {
        fail!("Not supported");
    }

    fn serialize_newtype_variant<T>(
        self,
        _name: &'static str,
        _variant_index: u32,
        _variant: &'static str,
        _value: &T,
    ) -> Result<()>
    where
        T: ?Sized + Serialize,
    {
        fail!("Not supported");
    }

    fn serialize_seq(self, _len: Option<usize>) -> Result<Self::SerializeSeq> {
        self.state = State::OuterSequence;
        Ok(self)
    }

    fn serialize_tuple(self, _len: usize) -> Result<Self::SerializeTuple> {
        fail!("Not supported");
    }

    fn serialize_tuple_struct(
        self,
        _name: &'static str,
        _len: usize,
    ) -> Result<Self::SerializeTupleStruct> {
        fail!("Not supported");
    }

    fn serialize_tuple_variant(
        self,
        _name: &'static str,
        _variant_index: u32,
        _variant: &'static str,
        _len: usize,
    ) -> Result<Self::SerializeTupleVariant> {
        fail!("Not supported");
    }

    fn serialize_map(self, _len: Option<usize>) -> Result<Self::SerializeMap> {
        self.record_serializer.start()?;
        Ok(self)
    }

    fn serialize_struct(self, _name: &'static str, _len: usize) -> Result<Self::SerializeStruct> {
        self.record_serializer.start()?;
        Ok(self)
    }

    fn serialize_struct_variant(
        self,
        _name: &'static str,
        _variant_index: u32,
        _variant: &'static str,
        _len: usize,
    ) -> Result<Self::SerializeStructVariant> {
        fail!("Not supported");
    }
}

impl<'a, S: RecordBuilder> SerializeSeq for &'a mut OuterSerializer<S> {
    type Ok = ();
    type Error = Error;

    fn serialize_element<T>(&mut self, value: &T) -> Result<()>
    where
        T: ?Sized + Serialize,
    {
        self.state = State::Item;
        value.serialize(&mut **self)?;
        self.state = State::OuterSequence;
        Ok(())
    }

    fn end(self) -> Result<()> {
        self.state = State::End;
        Ok(())
    }
}

impl<'a, S: RecordBuilder> SerializeStruct for &'a mut OuterSerializer<S> {
    type Ok = ();
    type Error = Error;

    fn serialize_field<T>(&mut self, key: &'static str, value: &T) -> Result<()>
    where
        T: ?Sized + Serialize,
    {
        self.record_serializer.field(key, value)?;
        Ok(())
    }

    fn end(self) -> Result<()> {
        self.record_serializer.end()?;
        Ok(())
    }
}

impl<'a, S: RecordBuilder> SerializeMap for &'a mut OuterSerializer<S> {
    type Ok = ();
    type Error = Error;

    fn serialize_key<T: Serialize + ?Sized>(&mut self, key: &T) -> Result<(), Self::Error> {
        if !matches!(self.state, State::Item) {
            fail!("Cannot enter a map field outside of an item");
        }

        let key = key.serialize(StringExtractor)?;
        self.state = State::Field(key);

        Ok(())
    }

    fn serialize_value<T: Serialize + ?Sized>(&mut self, value: &T) -> Result<(), Self::Error> {
        let field = match &self.state {
            State::Field(field) => field.to_owned(),
            _ => fail!("Cannot enter a map field outside of a sequence"),
        };

        self.record_serializer.field(&field, value)?;

        self.state = State::Item;
        Ok(())
    }

    fn end(self) -> Result<Self::Ok, Self::Error> {
        self.record_serializer.end()?;
        Ok(())
    }
}
