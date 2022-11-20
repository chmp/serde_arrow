//! The underlying data format used to interact with serde
//!

use crate::{base::error::fail, Error, Result};

/// The events used to interact with serde
///
/// The events model a JSON-like format that can be generated from objects
/// implementing `Serialize` and used to create objects that implement
/// `Deserialize`.
///
/// There are corresponding owned events for borrowed events (`OwnedStr` for
/// `Str` and `OwnedKey` for `Key`). To normalize to the borrowed events or to
/// the owned events use `event.to_self()` or `event.to_static` respectively.
/// For equality borrowed and owned events are considered equal.
///
#[derive(Debug, Clone)]
pub enum Event<'a> {
    /// Start a sequence, corresponds to `[` in JSON
    StartSequence,
    /// End a sequence, corresponds to `]` in JSON
    EndSequence,
    /// Start a struct, corresponds to `{` in JSON
    StartStruct,
    /// End a struct or struct, corresponds to `}` in JSON
    EndStruct,
    /// Indicate that the next event encodes a present value
    Some,
    /// A missing value
    Null,
    /// A borrowed key in a struct
    Key(&'a str),
    /// The owned variant of `Key`
    OwnedKey(String),
    /// A borrowed string
    Str(&'a str),
    /// The owned variant of `Str`
    OwnedStr(String),
    Bool(bool),
    I8(i8),
    I16(i16),
    I32(i32),
    I64(i64),
    U8(u8),
    U16(u16),
    U32(u32),
    U64(u64),
    F32(f32),
    F64(f64),
}

impl<'a> std::fmt::Display for Event<'a> {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Event::StartSequence => write!(f, "StartSequence"),
            Event::StartStruct => write!(f, "StartMap"),
            Event::Key(k) => write!(f, "Key({k:?})"),
            Event::OwnedKey(k) => write!(f, "OwnedKey({k:?})"),
            Event::Some => write!(f, "Some"),
            Event::Bool(v) => write!(f, "Bool({v})"),
            Event::I8(v) => write!(f, "I8({v})"),
            Event::I16(v) => write!(f, "I16({v})"),
            Event::I32(v) => write!(f, "I32({v})"),
            Event::I64(v) => write!(f, "I64({v})"),
            Event::U8(v) => write!(f, "U8({v})"),
            Event::U16(v) => write!(f, "U16({v})"),
            Event::U32(v) => write!(f, "U32({v})"),
            Event::U64(v) => write!(f, "U64({v})"),
            Event::F32(v) => write!(f, "F32({v})"),
            Event::F64(v) => write!(f, "F64({v})"),
            Event::Str(v) => write!(f, "Str({v:?})"),
            Event::OwnedStr(v) => write!(f, "String({v:?})"),
            Event::Null => write!(f, "Null"),
            Event::EndStruct => write!(f, "EndMap"),
            Event::EndSequence => write!(f, "EndSequence"),
        }
    }
}

impl<'this, 'other> std::cmp::PartialEq<Event<'other>> for Event<'this> {
    fn eq(&self, other: &Event<'other>) -> bool {
        use Event::*;
        match self {
            StartSequence => matches!(other, StartSequence),
            StartStruct => matches!(other, StartStruct),
            Null => matches!(other, Null),
            EndStruct => matches!(other, EndStruct),
            EndSequence => matches!(other, EndSequence),
            Key(s) => match other {
                Key(o) => s == o,
                OwnedKey(o) => s == o,
                _ => false,
            },
            OwnedKey(s) => match other {
                Key(o) => s == o,
                OwnedKey(o) => s == o,
                _ => false,
            },
            Str(s) => match other {
                Str(o) => s == o,
                OwnedStr(o) => s == o,
                _ => false,
            },
            OwnedStr(s) => match other {
                Str(o) => s == o,
                OwnedStr(o) => s == o,
                _ => false,
            },
            Some => matches!(other, Some),
            Bool(s) => matches!(other, Bool(o) if s == o),
            I8(s) => matches!(other, I8(o) if s == o),
            I16(s) => matches!(other, I16(o) if s == o),
            I32(s) => matches!(other, I32(o) if s == o),
            I64(s) => matches!(other, I64(o) if s == o),
            U8(s) => matches!(other, U8(o) if s == o),
            U16(s) => matches!(other, U16(o) if s == o),
            U32(s) => matches!(other, U32(o) if s == o),
            U64(s) => matches!(other, U64(o) if s == o),
            F32(s) => matches!(other, F32(o) if s == o),
            F64(s) => matches!(other, F64(o) if s == o),
        }
    }
}

impl<'a> Event<'a> {
    pub fn into_option<T: TryFrom<Event<'a>, Error = Error>>(self) -> Result<Option<T>> {
        match self {
            Event::Null => Ok(None),
            ev => Ok(Some(ev.try_into()?)),
        }
    }

    /// shorten the lifetime of the event
    pub fn to_self(&self) -> Event<'_> {
        match self {
            Event::OwnedKey(k) => Event::Key(k),
            Event::OwnedStr(s) => Event::Str(s),
            Event::Str(s) => Event::Str(s),
            Event::Key(k) => Event::Key(k),
            Event::StartSequence => Event::StartSequence,
            Event::StartStruct => Event::StartStruct,
            Event::Some => Event::Some,
            &Event::Bool(b) => Event::Bool(b),
            &Event::I8(v) => Event::I8(v),
            &Event::I16(v) => Event::I16(v),
            &Event::I32(v) => Event::I32(v),
            &Event::I64(v) => Event::I64(v),
            &Event::U8(v) => Event::U8(v),
            &Event::U16(v) => Event::U16(v),
            &Event::U32(v) => Event::U32(v),
            &Event::U64(v) => Event::U64(v),
            &Event::F32(v) => Event::F32(v),
            &Event::F64(v) => Event::F64(v),
            Event::Null => Event::Null,
            Event::EndStruct => Event::EndStruct,
            Event::EndSequence => Event::EndSequence,
        }
    }

    /// Increase the lifetime of the event to static
    ///
    /// This function clones any borrowed strings.
    ///
    pub fn to_static(&self) -> Event<'static> {
        match self {
            &Event::Str(s) => Event::OwnedStr(s.to_owned()),
            &Event::Key(k) => Event::OwnedKey(k.to_owned()),
            Event::StartSequence => Event::StartSequence,
            Event::StartStruct => Event::StartStruct,
            Event::OwnedKey(k) => Event::OwnedKey(k.to_owned()),
            Event::Some => Event::Some,
            &Event::Bool(b) => Event::Bool(b),
            &Event::I8(v) => Event::I8(v),
            &Event::I16(v) => Event::I16(v),
            &Event::I32(v) => Event::I32(v),
            &Event::I64(v) => Event::I64(v),
            &Event::U8(v) => Event::U8(v),
            &Event::U16(v) => Event::U16(v),
            &Event::U32(v) => Event::U32(v),
            &Event::U64(v) => Event::U64(v),
            &Event::F32(v) => Event::F32(v),
            &Event::F64(v) => Event::F64(v),
            Event::OwnedStr(v) => Event::OwnedStr(v.clone()),
            Event::Null => Event::Null,
            Event::EndStruct => Event::EndStruct,
            Event::EndSequence => Event::EndSequence,
        }
    }
}

macro_rules! event_implement_simple_from {
    ($ty:ty, $variant:ident) => {
        impl<'a> From<$ty> for Event<'a> {
            fn from(val: $ty) -> Self {
                Self::$variant(val)
            }
        }
    };
}

event_implement_simple_from!(bool, Bool);
event_implement_simple_from!(i8, I8);
event_implement_simple_from!(i16, I16);
event_implement_simple_from!(i32, I32);
event_implement_simple_from!(i64, I64);
event_implement_simple_from!(u8, U8);
event_implement_simple_from!(u16, U16);
event_implement_simple_from!(u32, U32);
event_implement_simple_from!(u64, U64);
event_implement_simple_from!(f32, F32);
event_implement_simple_from!(f64, F64);
event_implement_simple_from!(String, OwnedStr);

impl<'a> From<&'a str> for Event<'a> {
    fn from(val: &'a str) -> Event<'a> {
        Self::Str(val)
    }
}

macro_rules! event_implement_simple_try_from {
    ($ty:ty, $variant:ident) => {
        impl<'a> TryFrom<Event<'a>> for $ty {
            type Error = Error;
            fn try_from(val: Event<'_>) -> Result<$ty> {
                match val {
                    Event::$variant(val) => Ok(val),
                    // TODO: improve error message
                    event => fail!("Invalid conversion from {}", event),
                }
            }
        }
    };
}

event_implement_simple_try_from!(bool, Bool);
event_implement_simple_try_from!(i8, I8);
event_implement_simple_try_from!(i16, I16);
event_implement_simple_try_from!(i32, I32);
event_implement_simple_try_from!(i64, I64);
event_implement_simple_try_from!(u8, U8);
event_implement_simple_try_from!(u16, U16);
event_implement_simple_try_from!(u32, U32);
event_implement_simple_try_from!(u64, U64);
event_implement_simple_try_from!(f32, F32);
event_implement_simple_try_from!(f64, F64);

impl<'a> TryFrom<Event<'a>> for String {
    type Error = Error;
    fn try_from(val: Event<'_>) -> Result<String> {
        match val {
            Event::Str(val) => Ok(val.to_owned()),
            Event::OwnedStr(val) => Ok(val),
            event => fail!("Cannot convert {} to string", event),
        }
    }
}
