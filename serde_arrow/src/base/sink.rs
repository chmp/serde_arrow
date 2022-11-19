use serde::ser::{
    Impossible, SerializeMap, SerializeSeq, SerializeStruct, SerializeTuple, Serializer,
};
use serde::Serialize;

use crate::{fail, Error, Result};

use super::Event;

pub fn serialize_into_sink<T: Serialize + ?Sized, S: EventSink>(
    sink: &mut S,
    value: &T,
) -> Result<()> {
    value.serialize(EventSerializer(sink))?;
    Ok(())
}

pub trait EventSink {
    fn accept(&mut self, event: Event<'_>) -> Result<()>;
}

impl EventSink for Vec<Event<'static>> {
    fn accept(&mut self, event: Event<'_>) -> Result<()> {
        self.push(event.to_static());
        Ok(())
    }
}

struct EventSerializer<'a, S>(&'a mut S);

impl<'a, S: EventSink> Serializer for EventSerializer<'a, S> {
    type Ok = ();
    type Error = Error;

    type SerializeSeq = Self;
    type SerializeStruct = Self;
    type SerializeTuple = Self;
    type SerializeTupleStruct = Impossible<Self::Ok, Self::Error>;
    type SerializeTupleVariant = Impossible<Self::Ok, Self::Error>;
    type SerializeMap = Self;
    type SerializeStructVariant = Impossible<Self::Ok, Self::Error>;

    fn serialize_bool(self, val: bool) -> Result<()> {
        self.0.accept(val.into())
    }

    fn serialize_i8(self, val: i8) -> Result<()> {
        self.0.accept(val.into())
    }

    fn serialize_i16(self, val: i16) -> Result<()> {
        self.0.accept(val.into())
    }

    fn serialize_i32(self, val: i32) -> Result<()> {
        self.0.accept(val.into())
    }

    fn serialize_i64(self, val: i64) -> Result<()> {
        self.0.accept(val.into())
    }

    fn serialize_u8(self, val: u8) -> Result<()> {
        self.0.accept(val.into())
    }

    fn serialize_u16(self, val: u16) -> Result<()> {
        self.0.accept(val.into())
    }

    fn serialize_u32(self, val: u32) -> Result<()> {
        self.0.accept(val.into())
    }

    fn serialize_u64(self, val: u64) -> Result<()> {
        self.0.accept(val.into())
    }

    fn serialize_f32(self, val: f32) -> Result<()> {
        self.0.accept(val.into())
    }

    fn serialize_f64(self, val: f64) -> Result<()> {
        self.0.accept(val.into())
    }

    fn serialize_char(self, val: char) -> Result<()> {
        self.0.accept(u32::from(val).into())
    }

    fn serialize_str(self, val: &str) -> Result<()> {
        self.0.accept(val.into())
    }

    fn serialize_bytes(self, _val: &[u8]) -> Result<()> {
        fail!("serialize_bytes: cannot convert bytes to events")
    }

    fn serialize_none(self) -> Result<()> {
        self.0.accept(Event::Null)
    }

    fn serialize_some<T: ?Sized + Serialize>(self, value: &T) -> Result<()> {
        self.0.accept(Event::Some)?;
        value.serialize(self)
    }

    fn serialize_unit(self) -> Result<()> {
        self.0.accept(Event::Null)
    }

    fn serialize_unit_struct(self, _name: &'static str) -> Result<()> {
        self.serialize_unit()
    }

    fn serialize_unit_variant(
        self,
        _name: &'static str,
        _variant_index: u32,
        _variant: &'static str,
    ) -> Result<()> {
        fail!("serialize_unit_variant not supported");
    }

    fn serialize_newtype_struct<T: ?Sized + Serialize>(
        self,
        _name: &'static str,
        _value: &T,
    ) -> Result<()> {
        fail!("serialize_newtype_struct not supported");
    }

    fn serialize_newtype_variant<T: ?Sized + Serialize>(
        self,
        _name: &'static str,
        _variant_index: u32,
        _variant: &'static str,
        _value: &T,
    ) -> Result<()> {
        fail!("serialize_newtype_variant not supported");
    }

    fn serialize_seq(self, _len: Option<usize>) -> Result<Self::SerializeSeq> {
        self.0.accept(Event::StartSequence)?;
        Ok(self)
    }

    fn serialize_tuple(self, _len: usize) -> Result<Self::SerializeTuple> {
        self.0.accept(Event::StartSequence)?;
        Ok(self)
    }

    fn serialize_tuple_struct(
        self,
        _name: &'static str,
        _len: usize,
    ) -> Result<Self::SerializeTupleStruct> {
        fail!("serialize_tuple_struct not supported");
    }

    fn serialize_tuple_variant(
        self,
        _name: &'static str,
        _variant_index: u32,
        _variant: &'static str,
        _len: usize,
    ) -> Result<Self::SerializeTupleVariant> {
        fail!("serialize_tuple_variant not supported");
    }

    fn serialize_map(self, _len: Option<usize>) -> Result<Self::SerializeMap> {
        self.0.accept(Event::StartMap)?;
        Ok(self)
    }

    fn serialize_struct(self, _name: &'static str, _len: usize) -> Result<Self::SerializeStruct> {
        self.0.accept(Event::StartMap)?;
        Ok(self)
    }

    fn serialize_struct_variant(
        self,
        _name: &'static str,
        _variant_index: u32,
        _variant: &'static str,
        _len: usize,
    ) -> Result<Self::SerializeStructVariant> {
        fail!("serialize_struct_variant not supported");
    }
}

impl<'a, S: EventSink> SerializeSeq for EventSerializer<'a, S> {
    type Ok = ();
    type Error = Error;

    fn serialize_element<T>(&mut self, value: &T) -> Result<()>
    where
        T: ?Sized + Serialize,
    {
        value.serialize(EventSerializer(&mut *self.0))?;
        Ok(())
    }

    fn end(self) -> Result<()> {
        self.0.accept(Event::EndSequence)?;
        Ok(())
    }
}

impl<'a, S: EventSink> SerializeTuple for EventSerializer<'a, S> {
    type Ok = ();
    type Error = Error;

    fn serialize_element<T: ?Sized + Serialize>(&mut self, value: &T) -> Result<(), Self::Error> {
        value.serialize(EventSerializer(&mut *self.0))?;
        Ok(())
    }

    fn end(self) -> Result<Self::Ok, Self::Error> {
        self.0.accept(Event::EndSequence)?;
        Ok(())
    }
}

impl<'a, S: EventSink> SerializeStruct for EventSerializer<'a, S> {
    type Ok = ();
    type Error = Error;

    fn serialize_field<T>(&mut self, key: &'static str, value: &T) -> Result<()>
    where
        T: ?Sized + Serialize,
    {
        self.0.accept(Event::Key(key))?;
        value.serialize(EventSerializer(&mut *self.0))?;
        Ok(())
    }

    fn end(self) -> Result<()> {
        self.0.accept(Event::EndMap)?;
        Ok(())
    }
}

impl<'a, S: EventSink> SerializeMap for EventSerializer<'a, S> {
    type Ok = ();
    type Error = Error;

    fn serialize_key<T: Serialize + ?Sized>(&mut self, key: &T) -> Result<(), Self::Error> {
        key.serialize(EventSerializer(&mut *self.0))?;
        Ok(())
    }

    fn serialize_value<T: Serialize + ?Sized>(&mut self, value: &T) -> Result<(), Self::Error> {
        value.serialize(EventSerializer(&mut *self.0))?;
        Ok(())
    }

    fn end(self) -> Result<Self::Ok, Self::Error> {
        self.0.accept(Event::EndMap)?;
        Ok(())
    }
}
