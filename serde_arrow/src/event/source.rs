use crate::{error, event::Event, fail, Error, Result};

use serde::{
    de::{self, DeserializeSeed, MapAccess, SeqAccess, Visitor},
    Deserialize,
};

pub trait EventSource {
    /// Return whether the source has been fully consumed
    ///
    fn is_done(&self) -> bool;

    /// Peek at the next event without changing the internal state
    ///
    fn peek(&self) -> Option<Event<'_>>;

    /// Get the next event
    ///
    fn next_event(&mut self) -> Result<Event<'_>>;
}

pub trait IntoEventSource {
    type EventSource: EventSource;

    fn into_event_source(self) -> Self::EventSource;
}

impl<S: EventSource> IntoEventSource for S {
    type EventSource = Self;

    fn into_event_source(self) -> Self::EventSource {
        self
    }
}

pub fn deserialize_from_source<'de, T: Deserialize<'de>, S: IntoEventSource>(
    source: S,
) -> Result<T> {
    let mut deserializer = Deserializer {
        source: source.into_event_source(),
    };
    let res = T::deserialize(&mut deserializer)?;

    if !deserializer.source.is_done() {
        fail!("from_record_batch: Trailing content");
    }

    Ok(res)
}

pub struct Deserializer<S> {
    source: S,
}

impl<'de, 'a, S: EventSource> de::Deserializer<'de> for &'a mut Deserializer<S> {
    type Error = Error;

    fn deserialize_any<V: Visitor<'de>>(self, visitor: V) -> Result<V::Value> {
        match self.source.peek() {
            Some(Event::Bool(_)) => self.deserialize_bool(visitor),
            Some(Event::I8(_)) => self.deserialize_i8(visitor),
            Some(Event::I16(_)) => self.deserialize_i16(visitor),
            Some(Event::I32(_)) => self.deserialize_i32(visitor),
            Some(Event::I64(_)) => self.deserialize_i64(visitor),
            Some(Event::U8(_)) => self.deserialize_u8(visitor),
            Some(Event::U16(_)) => self.deserialize_u16(visitor),
            Some(Event::U32(_)) => self.deserialize_u32(visitor),
            Some(Event::U64(_)) => self.deserialize_u64(visitor),
            Some(Event::F32(_)) => self.deserialize_f32(visitor),
            Some(Event::F64(_)) => self.deserialize_f64(visitor),
            Some(Event::Str(_)) => self.deserialize_str(visitor),
            Some(Event::String(_)) => self.deserialize_string(visitor),
            Some(Event::StartMap) => self.deserialize_map(visitor),
            Some(Event::StartSequence) => self.deserialize_seq(visitor),
            ev => fail!("Invalid event in deserialize_any: {:?}", ev),
        }
    }

    fn deserialize_bool<V: Visitor<'de>>(self, visitor: V) -> Result<V::Value> {
        visitor.visit_bool(self.source.next_event()?.try_into()?)
    }

    fn deserialize_i8<V: Visitor<'de>>(self, visitor: V) -> Result<V::Value> {
        visitor.visit_i8(self.source.next_event()?.try_into()?)
    }

    fn deserialize_i16<V: Visitor<'de>>(self, visitor: V) -> Result<V::Value> {
        visitor.visit_i16(self.source.next_event()?.try_into()?)
    }

    fn deserialize_i32<V: Visitor<'de>>(self, visitor: V) -> Result<V::Value> {
        visitor.visit_i32(self.source.next_event()?.try_into()?)
    }

    fn deserialize_i64<V: Visitor<'de>>(self, visitor: V) -> Result<V::Value> {
        visitor.visit_i64(self.source.next_event()?.try_into()?)
    }

    fn deserialize_u8<V: Visitor<'de>>(self, visitor: V) -> Result<V::Value> {
        visitor.visit_u8(self.source.next_event()?.try_into()?)
    }

    fn deserialize_u16<V: Visitor<'de>>(self, visitor: V) -> Result<V::Value> {
        visitor.visit_u16(self.source.next_event()?.try_into()?)
    }

    fn deserialize_u32<V: Visitor<'de>>(self, visitor: V) -> Result<V::Value> {
        visitor.visit_u32(self.source.next_event()?.try_into()?)
    }

    fn deserialize_u64<V: Visitor<'de>>(self, visitor: V) -> Result<V::Value> {
        visitor.visit_u64(self.source.next_event()?.try_into()?)
    }

    fn deserialize_f32<V: Visitor<'de>>(self, visitor: V) -> Result<V::Value> {
        visitor.visit_f32(self.source.next_event()?.try_into()?)
    }

    fn deserialize_f64<V: Visitor<'de>>(self, visitor: V) -> Result<V::Value> {
        visitor.visit_f64(self.source.next_event()?.try_into()?)
    }

    fn deserialize_char<V: Visitor<'de>>(self, visitor: V) -> Result<V::Value> {
        match self.source.next_event()? {
            Event::U32(val) => {
                visitor.visit_char(char::from_u32(val).ok_or_else(|| error!("Invalid character"))?)
            }
            ev => fail!(
                "Invalid event {}, expected a character encoded as uint32",
                ev
            ),
        }
    }

    fn deserialize_str<V: Visitor<'de>>(self, visitor: V) -> Result<V::Value> {
        match self.source.next_event()? {
            Event::Key(key) => visitor.visit_str(key),
            Event::OwnedKey(key) => visitor.visit_str(&key),
            Event::Str(val) => visitor.visit_str(val),
            Event::String(val) => visitor.visit_str(&val),
            ev => fail!("Invalid event {}, expected str", ev),
        }
    }

    fn deserialize_string<V: Visitor<'de>>(self, visitor: V) -> Result<V::Value> {
        match self.source.next_event()? {
            Event::Key(key) => visitor.visit_string(key.to_owned()),
            Event::OwnedKey(key) => visitor.visit_str(&key),
            Event::Str(val) => visitor.visit_string(val.to_owned()),
            Event::String(val) => visitor.visit_string(val),
            ev => fail!("Invalid event {}, expected string", ev),
        }
    }

    fn deserialize_bytes<V: Visitor<'de>>(self, _visitor: V) -> Result<V::Value> {
        fail!("deserialize_bytes: Bytes are not supported at the moment")
    }

    fn deserialize_byte_buf<V: Visitor<'de>>(self, _visitor: V) -> Result<V::Value> {
        fail!("deserialize_byte_buf: Bytes are not supported at the moment")
    }

    fn deserialize_option<V: Visitor<'de>>(self, visitor: V) -> Result<V::Value> {
        if let Some(Event::Null) = self.source.peek() {
            self.source.next_event()?;
            visitor.visit_none()
        } else {
            // Support deserializing options both with and without Some markers
            if let Some(Event::Some) = self.source.peek() {
                self.source.next_event()?;
            }

            visitor.visit_some(self)
        }
    }

    fn deserialize_unit<V: Visitor<'de>>(self, visitor: V) -> Result<V::Value> {
        match self.source.next_event()? {
            Event::Null => visitor.visit_unit(),
            ev => fail!("deserialize_unit: Cannot handle {}", ev),
        }
    }

    fn deserialize_unit_struct<V: Visitor<'de>>(
        self,
        _name: &'static str,
        visitor: V,
    ) -> Result<V::Value> {
        self.deserialize_unit(visitor)
    }

    fn deserialize_newtype_struct<V: Visitor<'de>>(
        self,
        _name: &'static str,
        visitor: V,
    ) -> Result<V::Value> {
        visitor.visit_newtype_struct(self)
    }

    fn deserialize_seq<V: Visitor<'de>>(self, visitor: V) -> Result<V::Value> {
        if !matches!(self.source.next_event()?, Event::StartSequence) {
            fail!("Expected start of sequence");
        }

        let res = visitor.visit_seq(&mut *self)?;

        if !matches!(self.source.next_event()?, Event::EndSequence) {
            fail!("Expected end of sequence");
        }
        Ok(res)
    }

    fn deserialize_tuple<V: Visitor<'de>>(self, _len: usize, visitor: V) -> Result<V::Value> {
        self.deserialize_seq(visitor)
    }

    fn deserialize_tuple_struct<V: Visitor<'de>>(
        self,
        _name: &'static str,
        _len: usize,
        visitor: V,
    ) -> Result<V::Value> {
        self.deserialize_seq(visitor)
    }

    fn deserialize_map<V: Visitor<'de>>(self, visitor: V) -> Result<V::Value> {
        if !matches!(self.source.next_event()?, Event::StartMap) {
            fail!("Expected start of map");
        }

        let res = visitor.visit_map(&mut *self)?;

        if !matches!(self.source.next_event()?, Event::EndMap) {
            fail!("Expected end of map");
        }
        Ok(res)
    }

    fn deserialize_struct<V: Visitor<'de>>(
        self,
        _name: &'static str,
        _fields: &'static [&'static str],
        visitor: V,
    ) -> Result<V::Value> {
        self.deserialize_map(visitor)
    }

    fn deserialize_enum<V: Visitor<'de>>(
        self,
        _name: &'static str,
        _variants: &'static [&'static str],
        _visitor: V,
    ) -> Result<V::Value> {
        fail!("deserialize_enum: Enums are not supported at the moment")
    }

    fn deserialize_identifier<V: Visitor<'de>>(self, visitor: V) -> Result<V::Value> {
        self.deserialize_str(visitor)
    }

    fn deserialize_ignored_any<V: Visitor<'de>>(self, visitor: V) -> Result<V::Value> {
        self.deserialize_any(visitor)
    }
}

impl<'de, 'a, S: EventSource> SeqAccess<'de> for &'a mut Deserializer<S> {
    type Error = Error;

    fn next_element_seed<T>(&mut self, seed: T) -> Result<Option<T::Value>>
    where
        T: DeserializeSeed<'de>,
    {
        if matches!(self.source.peek(), Some(Event::EndSequence)) {
            return Ok(None);
        }
        seed.deserialize(&mut **self).map(Some)
    }
}

impl<'de, 'a, S: EventSource> MapAccess<'de> for &'a mut Deserializer<S> {
    type Error = Error;

    fn next_key_seed<K>(&mut self, seed: K) -> Result<Option<K::Value>>
    where
        K: DeserializeSeed<'de>,
    {
        if matches!(self.source.peek(), Some(Event::EndMap)) {
            return Ok(None);
        }
        seed.deserialize(&mut **self).map(Some)
    }

    fn next_value_seed<V>(&mut self, seed: V) -> Result<V::Value>
    where
        V: DeserializeSeed<'de>,
    {
        seed.deserialize(&mut **self)
    }
}

pub struct SliceSource<'items, 'event> {
    items: &'items [Event<'event>],
    next: usize,
}

impl<'items, 'event> EventSource for SliceSource<'items, 'event> {
    fn is_done(&self) -> bool {
        self.next == self.items.len()
    }

    fn peek(&self) -> Option<Event<'event>> {
        self.items.get(self.next).cloned()
    }

    fn next_event(&mut self) -> Result<Event<'event>> {
        let res = self
            .items
            .get(self.next)
            .cloned()
            .ok_or_else(|| error!("no next event"));
        self.next += 1;
        res
    }
}

impl<'items, 'event> IntoEventSource for &'items [Event<'event>] {
    type EventSource = SliceSource<'items, 'event>;

    fn into_event_source(self) -> Self::EventSource {
        SliceSource {
            items: self,
            next: 0,
        }
    }
}

impl<'items, 'event> IntoEventSource for &'items Vec<Event<'event>> {
    type EventSource = SliceSource<'items, 'event>;

    fn into_event_source(self) -> Self::EventSource {
        SliceSource {
            items: self.as_slice(),
            next: 0,
        }
    }
}
