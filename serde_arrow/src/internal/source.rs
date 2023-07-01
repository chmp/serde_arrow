use std::borrow::Cow;

use serde::de::{
    self, Deserialize, DeserializeSeed, EnumAccess, MapAccess, SeqAccess, VariantAccess, Visitor,
};

use crate::internal::{
    error::{error, fail, Error, Result},
    event::Event,
};

/// Deserialize a type from an [EventSource]
///
/// This function may be helpful when creating custom formats.
///
pub fn deserialize_from_source<
    'de,
    'event,
    T: Deserialize<'de>,
    S: IntoEventSource<'event> + 'event,
>(
    source: S,
) -> Result<T> {
    let mut deserializer = Deserializer {
        source: PeekableEventSource::new(source.into_event_source()),
    };
    let res = T::deserialize(&mut deserializer)?;

    if deserializer.source.next()?.is_some() {
        fail!("from_record_batch: Trailing content");
    }

    Ok(res)
}

/// A source of [Events][Event] that can be used to deserialize rust objects
///
/// **Note**: implementations are not expected to yield `Some` events for
/// nullable values, as the nullability can be recovered from the underlying
/// arrays. This relaxed relaxed requirements implies that schema tracing from
/// the Array sources may be unreliable.
///
pub trait EventSource<'a> {
    fn next(&mut self) -> Result<Option<Event<'a>>>;
}

pub struct PeekableEventSource<'a, S: EventSource<'a> + 'a> {
    source: S,
    peeked: Option<Option<Event<'a>>>,
}

impl<'a, S: EventSource<'a> + 'a> PeekableEventSource<'a, S> {
    pub fn new(source: S) -> Self {
        Self {
            source,
            peeked: None,
        }
    }

    pub fn peek(&mut self) -> Result<Option<Event<'a>>> {
        if let Some(peeked) = self.peeked.as_ref() {
            Ok(peeked.clone())
        } else {
            let ev = self.source.next()?;
            self.peeked = Some(ev.clone());
            Ok(ev)
        }
    }
}

impl<'a, S: EventSource<'a> + 'a> EventSource<'a> for PeekableEventSource<'a, S> {
    fn next(&mut self) -> Result<Option<Event<'a>>> {
        if let Some(peeked) = self.peeked.take() {
            Ok(peeked)
        } else {
            self.source.next()
        }
    }
}

pub struct DynamicSource<'a> {
    source: Box<dyn EventSource<'a> + 'a>,
}

impl<'a> DynamicSource<'a> {
    pub fn new<S: EventSource<'a> + 'a>(source: S) -> Self {
        Self {
            source: Box::new(source),
        }
    }
}

impl<'a> EventSource<'a> for DynamicSource<'a> {
    fn next(&mut self) -> Result<Option<Event<'a>>> {
        self.source.next()
    }
}

enum AddOuterSequenceState {
    Start,
    Inner,
    Done,
}

pub(crate) struct AddOuterSequenceSource<S> {
    wrapped: S,
    state: AddOuterSequenceState,
}

impl<S> AddOuterSequenceSource<S> {
    pub fn new(wrapped: S) -> Self {
        Self {
            wrapped,
            state: AddOuterSequenceState::Start,
        }
    }
}

impl<'a, S: EventSource<'a>> EventSource<'a> for AddOuterSequenceSource<S> {
    fn next(&mut self) -> Result<Option<Event<'a>>> {
        let res: Event<'a>;
        self.state = match self.state {
            AddOuterSequenceState::Start => {
                res = Event::StartSequence;
                AddOuterSequenceState::Inner
            }
            AddOuterSequenceState::Inner => {
                let cand = self.wrapped.next()?;
                if let Some(ev) = cand {
                    res = ev;
                    AddOuterSequenceState::Inner
                } else {
                    res = Event::EndSequence;
                    AddOuterSequenceState::Done
                }
            }
            AddOuterSequenceState::Done => return Ok(None),
        };

        Ok(Some(res))
    }
}

pub trait IntoEventSource<'a> {
    type EventSource: EventSource<'a>;

    fn into_event_source(self) -> Self::EventSource;
}

impl<'a, S: EventSource<'a>> IntoEventSource<'a> for S {
    type EventSource = Self;

    fn into_event_source(self) -> Self::EventSource {
        self
    }
}

pub struct Deserializer<'event, S: EventSource<'event>> {
    source: PeekableEventSource<'event, S>,
}

impl<'de, 'a, 'event, S: EventSource<'event>> de::Deserializer<'de>
    for &'a mut Deserializer<'event, S>
{
    type Error = Error;

    fn deserialize_any<V: Visitor<'de>>(self, visitor: V) -> Result<V::Value> {
        match self.source.peek()? {
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
            Some(Event::OwnedStr(_)) => self.deserialize_string(visitor),
            Some(Event::StartStruct) => self.deserialize_map(visitor),
            Some(Event::StartSequence) => self.deserialize_seq(visitor),
            Some(Event::Variant(_, _) | Event::OwnedVariant(_, _)) => {
                self.deserialize_enum("", &[], visitor)
            }
            Some(ev) => fail!("Invalid event in deserialize_any: Some({ev})"),
            None => fail!("Invalid event in deserialize_any: None"),
        }
    }

    fn deserialize_bool<V: Visitor<'de>>(self, visitor: V) -> Result<V::Value> {
        visitor.visit_bool(required(self.source.next()?)?.try_into()?)
    }

    fn deserialize_i8<V: Visitor<'de>>(self, visitor: V) -> Result<V::Value> {
        visitor.visit_i8(required(self.source.next()?)?.try_into()?)
    }

    fn deserialize_i16<V: Visitor<'de>>(self, visitor: V) -> Result<V::Value> {
        visitor.visit_i16(required(self.source.next()?)?.try_into()?)
    }

    fn deserialize_i32<V: Visitor<'de>>(self, visitor: V) -> Result<V::Value> {
        visitor.visit_i32(required(self.source.next()?)?.try_into()?)
    }

    fn deserialize_i64<V: Visitor<'de>>(self, visitor: V) -> Result<V::Value> {
        visitor.visit_i64(required(self.source.next()?)?.try_into()?)
    }

    fn deserialize_u8<V: Visitor<'de>>(self, visitor: V) -> Result<V::Value> {
        visitor.visit_u8(required(self.source.next()?)?.try_into()?)
    }

    fn deserialize_u16<V: Visitor<'de>>(self, visitor: V) -> Result<V::Value> {
        visitor.visit_u16(required(self.source.next()?)?.try_into()?)
    }

    fn deserialize_u32<V: Visitor<'de>>(self, visitor: V) -> Result<V::Value> {
        visitor.visit_u32(required(self.source.next()?)?.try_into()?)
    }

    fn deserialize_u64<V: Visitor<'de>>(self, visitor: V) -> Result<V::Value> {
        visitor.visit_u64(required(self.source.next()?)?.try_into()?)
    }

    fn deserialize_f32<V: Visitor<'de>>(self, visitor: V) -> Result<V::Value> {
        visitor.visit_f32(required(self.source.next()?)?.try_into()?)
    }

    fn deserialize_f64<V: Visitor<'de>>(self, visitor: V) -> Result<V::Value> {
        visitor.visit_f64(required(self.source.next()?)?.try_into()?)
    }

    fn deserialize_char<V: Visitor<'de>>(self, visitor: V) -> Result<V::Value> {
        match required(self.source.next()?)? {
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
        match required(self.source.next()?)? {
            Event::Str(val) => visitor.visit_str(val),
            Event::OwnedStr(val) => visitor.visit_str(&val),
            ev => fail!("Invalid event {}, expected str", ev),
        }
    }

    fn deserialize_string<V: Visitor<'de>>(self, visitor: V) -> Result<V::Value> {
        match required(self.source.next()?)? {
            Event::Str(val) => visitor.visit_string(val.to_owned()),
            Event::OwnedStr(val) => visitor.visit_string(val),
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
        if let Some(Event::Null) = self.source.peek()? {
            self.source.next()?;
            visitor.visit_none()
        } else {
            // Support deserializing options both with and without Some markers
            if let Some(Event::Some) = self.source.peek()? {
                self.source.next()?;
            }

            visitor.visit_some(self)
        }
    }

    fn deserialize_unit<V: Visitor<'de>>(self, visitor: V) -> Result<V::Value> {
        match required(self.source.next()?)? {
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
        if !matches!(self.source.next()?, Some(Event::StartSequence)) {
            fail!("Expected start of sequence");
        }

        let res = visitor.visit_seq(&mut *self)?;

        if !matches!(self.source.next()?, Some(Event::EndSequence)) {
            fail!("Expected end of sequence");
        }
        Ok(res)
    }

    fn deserialize_tuple<V: Visitor<'de>>(self, _len: usize, visitor: V) -> Result<V::Value> {
        if !matches!(self.source.next()?, Some(Event::StartTuple)) {
            fail!("Expected start of tuple");
        }

        let res = visitor.visit_seq(&mut *self)?;

        if !matches!(self.source.next()?, Some(Event::EndTuple)) {
            fail!("Expected end of tuple");
        }
        Ok(res)
    }

    fn deserialize_tuple_struct<V: Visitor<'de>>(
        self,
        _name: &'static str,
        _len: usize,
        visitor: V,
    ) -> Result<V::Value> {
        if !matches!(self.source.next()?, Some(Event::StartTuple)) {
            fail!("Expected start of tuple");
        }

        let res = visitor.visit_seq(&mut *self)?;

        if !matches!(self.source.next()?, Some(Event::EndTuple)) {
            fail!("Expected end of tuple");
        }
        Ok(res)
    }

    fn deserialize_map<V: Visitor<'de>>(self, visitor: V) -> Result<V::Value> {
        match self.source.next()? {
            Some(Event::StartMap) | Some(Event::StartStruct) => {}
            Some(ev) => fail!("Expected StartMap, got Some({ev})"),
            None => fail!("Expected StartMap, got None"),
        }

        let res = visitor.visit_map(&mut *self)?;

        match self.source.next()? {
            Some(Event::EndMap) | Some(Event::EndStruct) => {}
            Some(ev) => fail!("Expected EndMap, got Some({ev})"),
            None => fail!("Expected EndMap, got None"),
        }

        Ok(res)
    }

    fn deserialize_struct<V: Visitor<'de>>(
        self,
        _name: &'static str,
        _fields: &'static [&'static str],
        visitor: V,
    ) -> Result<V::Value> {
        if !matches!(self.source.next()?, Some(Event::StartStruct)) {
            fail!("Expected start of struct");
        }

        let res = visitor.visit_map(&mut *self)?;

        if !matches!(self.source.next()?, Some(Event::EndStruct)) {
            fail!("Expected end of struct");
        }
        Ok(res)
    }

    fn deserialize_enum<V: Visitor<'de>>(
        self,
        _name: &'static str,
        _variants: &'static [&'static str],
        visitor: V,
    ) -> Result<V::Value> {
        visitor.visit_enum(&mut *self)
    }

    fn deserialize_identifier<V: Visitor<'de>>(self, visitor: V) -> Result<V::Value> {
        self.deserialize_str(visitor)
    }

    fn deserialize_ignored_any<V: Visitor<'de>>(self, visitor: V) -> Result<V::Value> {
        self.deserialize_any(visitor)
    }
}

impl<'de, 'a, 'event, S: EventSource<'event>> SeqAccess<'de> for &'a mut Deserializer<'event, S> {
    type Error = Error;

    fn next_element_seed<T>(&mut self, seed: T) -> Result<Option<T::Value>>
    where
        T: DeserializeSeed<'de>,
    {
        if matches!(self.source.peek()?, Some(Event::EndSequence)) {
            return Ok(None);
        }
        // ignore event markers to be forwards compatible
        if matches!(self.source.peek()?, Some(Event::Item)) {
            self.source.next()?;
        }
        seed.deserialize(&mut **self).map(Some)
    }
}

impl<'de, 'a, 'event, S: EventSource<'event>> MapAccess<'de> for &'a mut Deserializer<'event, S> {
    type Error = Error;

    fn next_key_seed<K>(&mut self, seed: K) -> Result<Option<K::Value>>
    where
        K: DeserializeSeed<'de>,
    {
        match self.source.peek()? {
            Some(Event::EndStruct) | Some(Event::EndMap) => return Ok(None),
            // allow optional item markers. E.g., structs are currently
            // serialized without item markers.
            Some(Event::Item) => {
                self.source.next()?;
            }
            _ => {}
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

impl<'de, 'a, 'event, S: EventSource<'event>> EnumAccess<'de> for &'a mut Deserializer<'event, S> {
    type Error = Error;
    type Variant = Self;

    fn variant_seed<V>(self, seed: V) -> Result<(V::Value, Self::Variant), Self::Error>
    where
        V: DeserializeSeed<'de>,
    {
        let (name, idx) = match required(self.source.next()?)? {
            Event::Variant(name, idx) => (Cow::Borrowed(name), idx),
            Event::OwnedVariant(name, idx) => (Cow::Owned(name), idx),
            ev => fail!("variant_seed: Cannot handle {}", ev),
        };

        struct SeedDeserializer<'a> {
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

        impl<'de, 'a> de::Deserializer<'de> for SeedDeserializer<'a> {
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

        let val = seed.deserialize(SeedDeserializer {
            idx,
            name: name.as_ref(),
        })?;
        Ok((val, self))
    }
}

impl<'de, 'a, 'event, S: EventSource<'event>> VariantAccess<'de>
    for &'a mut Deserializer<'event, S>
{
    type Error = Error;

    fn newtype_variant_seed<T>(self, seed: T) -> Result<T::Value, Self::Error>
    where
        T: DeserializeSeed<'de>,
    {
        seed.deserialize(&mut *self)
    }

    fn struct_variant<V>(
        self,
        fields: &'static [&'static str],
        visitor: V,
    ) -> Result<V::Value, Self::Error>
    where
        V: Visitor<'de>,
    {
        de::Deserializer::deserialize_struct(self, "", fields, visitor)
    }

    fn tuple_variant<V>(self, len: usize, visitor: V) -> Result<V::Value, Self::Error>
    where
        V: Visitor<'de>,
    {
        de::Deserializer::deserialize_tuple(self, len, visitor)
    }

    fn unit_variant(self) -> Result<(), Self::Error> {
        match required(self.source.next()?)? {
            Event::Null => Ok(()),
            ev => fail!("deserialize_unit: Cannot handle {}", ev),
        }
    }
}

pub struct SliceSource<'items, 'event> {
    items: &'items [Event<'event>],
    next: usize,
}

impl<'items, 'event> EventSource<'event> for SliceSource<'items, 'event> {
    fn next(&mut self) -> Result<Option<Event<'event>>> {
        match self.items.get(self.next).cloned() {
            Some(next) => {
                self.next += 1;
                Ok(Some(next))
            }
            None => Ok(None),
        }
    }
}

impl<'items, 'event> IntoEventSource<'event> for &'items [Event<'event>] {
    type EventSource = SliceSource<'items, 'event>;

    fn into_event_source(self) -> Self::EventSource {
        SliceSource {
            items: self,
            next: 0,
        }
    }
}

impl<'items, 'event> IntoEventSource<'event> for &'items Vec<Event<'event>> {
    type EventSource = SliceSource<'items, 'event>;

    fn into_event_source(self) -> Self::EventSource {
        SliceSource {
            items: self.as_slice(),
            next: 0,
        }
    }
}

fn required(event: Option<Event<'_>>) -> Result<Event<'_>> {
    event.ok_or_else(|| error!("Unexpected no event"))
}
