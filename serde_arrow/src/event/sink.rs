use serde::ser::{
    Impossible, SerializeMap, SerializeSeq, SerializeStruct, SerializeTuple, Serializer,
};
use serde::Serialize;

use crate::{fail, Error, Result};

use super::Event;

pub fn serialize_into_sink<T: Serialize + ?Sized, S: EventSink>(sink: S, value: &T) -> Result<S> {
    let mut serializer = EventSerializer::new(sink);
    value.serialize(&mut serializer)?;
    Ok(serializer.into_sink())
}

pub trait EventSink {
    fn accept(&mut self, event: Event<'_>) -> Result<()>;
}

struct EventSerializer<S> {
    sink: S,
    expect_key: bool,
}

impl<S> EventSerializer<S> {
    fn new(sink: S) -> Self {
        Self {
            sink,
            expect_key: false,
        }
    }

    fn into_sink(self) -> S {
        self.sink
    }
}

impl<S: EventSink> EventSerializer<S> {
    fn accept(&mut self, event: Event<'_>) -> Result<()> {
        if self.expect_key {
            fail!("Expected a key, not a value");
        }
        self.sink.accept(event)
    }
}

impl<'a, S: EventSink> Serializer for &'a mut EventSerializer<S> {
    type Ok = ();
    type Error = Error;

    type SerializeSeq = Self;
    type SerializeStruct = Self;
    type SerializeTuple = Self;
    type SerializeTupleStruct = Impossible<Self::Ok, Self::Error>;
    type SerializeTupleVariant = Impossible<Self::Ok, Self::Error>;
    type SerializeMap = Self;
    type SerializeStructVariant = Impossible<Self::Ok, Self::Error>;

    fn serialize_bool(self, _val: bool) -> Result<()> {
        todo!()
    }

    fn serialize_i8(self, val: i8) -> Result<()> {
        self.accept(Event::I8(val))
    }

    fn serialize_i16(self, _val: i16) -> Result<()> {
        todo!()
    }

    fn serialize_i32(self, val: i32) -> Result<()> {
        self.accept(Event::I32(val))
    }

    fn serialize_i64(self, _val: i64) -> Result<()> {
        todo!()
    }

    fn serialize_u8(self, _val: u8) -> Result<()> {
        todo!()
    }

    fn serialize_u16(self, _val: u16) -> Result<()> {
        todo!()
    }

    fn serialize_u32(self, _val: u32) -> Result<()> {
        todo!()
    }

    fn serialize_u64(self, _val: u64) -> Result<()> {
        todo!()
    }

    fn serialize_f32(self, _val: f32) -> Result<()> {
        todo!()
    }

    fn serialize_f64(self, _val: f64) -> Result<()> {
        todo!()
    }

    fn serialize_char(self, _val: char) -> Result<()> {
        todo!()
    }

    fn serialize_str(self, _val: &str) -> Result<()> {
        todo!()
    }

    fn serialize_bytes(self, _val: &[u8]) -> Result<()> {
        todo!()
    }

    fn serialize_none(self) -> Result<()> {
        todo!()
    }

    fn serialize_some<T>(self, _value: &T) -> Result<()>
    where
        T: ?Sized + Serialize,
    {
        todo!()
    }

    fn serialize_unit(self) -> Result<()> {
        todo!()
    }

    fn serialize_unit_struct(self, _name: &'static str) -> Result<()> {
        todo!()
    }

    fn serialize_unit_variant(
        self,
        _name: &'static str,
        _variant_index: u32,
        _variant: &'static str,
    ) -> Result<()> {
        todo!()
    }

    fn serialize_newtype_struct<T>(self, _name: &'static str, _value: &T) -> Result<()>
    where
        T: ?Sized + Serialize,
    {
        todo!()
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
        todo!()
    }

    fn serialize_seq(self, _len: Option<usize>) -> Result<Self::SerializeSeq> {
        self.sink.accept(Event::StartSequence)?;
        Ok(self)
    }

    fn serialize_tuple(self, _len: usize) -> Result<Self::SerializeTuple> {
        self.sink.accept(Event::StartSequence)?;
        Ok(self)
    }

    fn serialize_tuple_struct(
        self,
        _name: &'static str,
        _len: usize,
    ) -> Result<Self::SerializeTupleStruct> {
        todo!()
    }

    fn serialize_tuple_variant(
        self,
        _name: &'static str,
        _variant_index: u32,
        _variant: &'static str,
        _len: usize,
    ) -> Result<Self::SerializeTupleVariant> {
        todo!()
    }

    fn serialize_map(self, _len: Option<usize>) -> Result<Self::SerializeMap> {
        self.sink.accept(Event::StartMap)?;
        Ok(self)
    }

    fn serialize_struct(self, _name: &'static str, _len: usize) -> Result<Self::SerializeStruct> {
        self.sink.accept(Event::StartMap)?;
        Ok(self)
    }

    fn serialize_struct_variant(
        self,
        _name: &'static str,
        _variant_index: u32,
        _variant: &'static str,
        _len: usize,
    ) -> Result<Self::SerializeStructVariant> {
        todo!()
    }
}

impl<'a, S: EventSink> SerializeSeq for &'a mut EventSerializer<S> {
    type Ok = ();
    type Error = Error;

    fn serialize_element<T>(&mut self, value: &T) -> Result<()>
    where
        T: ?Sized + Serialize,
    {
        value.serialize(&mut **self)?;
        Ok(())
    }

    fn end(self) -> Result<()> {
        self.sink.accept(Event::EndSequence)?;
        Ok(())
    }
}

impl<'a, S: EventSink> SerializeTuple for &'a mut EventSerializer<S> {
    type Ok = ();
    type Error = Error;

    fn serialize_element<T: ?Sized + Serialize>(&mut self, value: &T) -> Result<(), Self::Error> {
        value.serialize(&mut **self)?;
        Ok(())
    }

    fn end(self) -> Result<Self::Ok, Self::Error> {
        self.sink.accept(Event::EndSequence)?;
        Ok(())
    }
}

impl<'a, S: EventSink> SerializeStruct for &'a mut EventSerializer<S> {
    type Ok = ();
    type Error = Error;

    fn serialize_field<T>(&mut self, key: &'static str, value: &T) -> Result<()>
    where
        T: ?Sized + Serialize,
    {
        self.sink.accept(Event::Key(key))?;
        value.serialize(&mut **self)?;
        Ok(())
    }

    fn end(self) -> Result<()> {
        self.sink.accept(Event::EndMap)?;
        Ok(())
    }
}

impl<'a, S: EventSink> SerializeMap for &'a mut EventSerializer<S> {
    type Ok = ();
    type Error = Error;

    fn serialize_key<T: Serialize + ?Sized>(&mut self, key: &T) -> Result<(), Self::Error> {
        self.expect_key = true;
        key.serialize(&mut **self)?;
        if self.expect_key {
            fail!("Key not found");
        }
        Ok(())
    }

    fn serialize_value<T: Serialize + ?Sized>(&mut self, value: &T) -> Result<(), Self::Error> {
        value.serialize(&mut **self)?;
        Ok(())
    }

    fn end(self) -> Result<Self::Ok, Self::Error> {
        self.sink.accept(Event::EndMap)?;
        Ok(())
    }
}
