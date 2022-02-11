//! The underlying data format used to interact with serde
//!

use crate::{fail, Error, Result};

#[derive(Debug, PartialEq)]
pub enum Event<'a> {
    StartSequence,
    StartMap,
    Key(&'a str),
    Some,
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
    Str(&'a str),
    String(String),
    Null,
    EndMap,
    EndSequence,
}

impl<'a> std::fmt::Display for Event<'a> {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Event::StartSequence => write!(f, "StartSequence"),
            Event::StartMap => write!(f, "StartMap"),
            Event::Key(_) => write!(f, "Key"),
            Event::Some => write!(f, "Some"),
            Event::Bool(_) => write!(f, "Bool"),
            Event::I8(_) => write!(f, "I8"),
            Event::I16(_) => write!(f, "I16"),
            Event::I32(_) => write!(f, "I32"),
            Event::I64(_) => write!(f, "I64"),
            Event::U8(_) => write!(f, "U8"),
            Event::U16(_) => write!(f, "U16"),
            Event::U32(_) => write!(f, "U32"),
            Event::U64(_) => write!(f, "U64"),
            Event::F32(_) => write!(f, "F32"),
            Event::F64(_) => write!(f, "F64"),
            Event::Str(_) => write!(f, "Str"),
            Event::String(_) => write!(f, "String"),
            Event::Null => write!(f, "Null"),
            Event::EndMap => write!(f, "EndMap"),
            Event::EndSequence => write!(f, "EndSequence"),
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
event_implement_simple_from!(String, String);

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
            Event::String(val) => Ok(val),
            event => fail!("Cannot convert {} to string", event),
        }
    }
}
