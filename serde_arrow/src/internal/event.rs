//! The underlying data format used to interact with serde
//!

use crate::{internal::error::fail, Error, Result};

/// The events used to interact with serde
///
/// The events model a JSON-like format that can be generated from objects
/// implementing `Serialize` and used to create objects that implement
/// `Deserialize`.
///
/// For the borrow strings events (`Str`), there  are corresponding owned events
/// (`OwnedStr`). To normalize to the borrowed events or to the owned events use
/// `event.to_self()` or `event.to_static` respectively. For equality borrowed
/// and owned events are considered equal.
///
#[derive(Debug, Clone)]
pub enum Event<'a> {
    /// Start a sequence, corresponds to `[` in JSON
    StartSequence,
    /// End a sequence, corresponds to `]` in JSON
    EndSequence,
    /// Start a tuple, corresponds to `[` in JSON
    StartTuple,
    /// End a tuple, corresponds to `]` in JSON
    EndTuple,
    /// Start a struct, corresponds to `{` in JSON
    StartStruct,
    /// End a struct, corresponds to `}` in JSON
    EndStruct,
    /// Start a map, corresponds to `{` in JSON
    StartMap,
    /// End a map, corresponds to `}` in JSON
    EndMap,
    /// Indicate that the next event encodes a present value
    Some,
    /// A missing value
    Null,
    /// A borrowed string
    Str(&'a str),
    /// The owned variant of `Str`
    OwnedStr(String),
    /// Push the default of the current type
    Default,
    /// Denote the variant of a union
    Variant(&'a str, usize),
    /// The owned version of Variant
    OwnedVariant(String, usize),
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
            Event::EndSequence => write!(f, "EndSequence"),
            Event::StartTuple => write!(f, "StartTuple"),
            Event::EndTuple => write!(f, "EndTuple"),
            Event::StartStruct => write!(f, "StartStruct"),
            Event::EndStruct => write!(f, "EndStruct"),
            Event::StartMap => write!(f, "StartMap"),
            Event::EndMap => write!(f, "EndMap"),
            Event::Some => write!(f, "Some"),
            Event::Null => write!(f, "Null"),
            Event::Default => write!(f, "Default"),
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
            Event::Variant(n, i) => write!(f, "Variant({n:?}, {i})"),
            Event::OwnedVariant(n, i) => write!(f, "OwnedVariant({n:?}, {i})"),
            Event::Str(v) => write!(f, "Str({v:?})"),
            Event::OwnedStr(v) => write!(f, "String({v:?})"),
        }
    }
}

impl<'this, 'other> std::cmp::PartialEq<Event<'other>> for Event<'this> {
    fn eq(&self, other: &Event<'other>) -> bool {
        use Event::*;
        match self {
            StartSequence => matches!(other, StartSequence),
            EndSequence => matches!(other, EndSequence),
            StartStruct => matches!(other, StartStruct),
            EndStruct => matches!(other, EndStruct),
            StartTuple => matches!(other, StartTuple),
            EndTuple => matches!(other, EndTuple),
            StartMap => matches!(other, StartMap),
            EndMap => matches!(other, EndMap),
            Default => matches!(other, Default),
            Null => matches!(other, Null),
            Variant(n, i) => match other {
                Variant(on, oi) if on == n && oi == i => true,
                OwnedVariant(on, oi) if on == n && oi == i => true,
                _ => false,
            },
            OwnedVariant(n, i) => match other {
                Variant(on, oi) if on == n && oi == i => true,
                OwnedVariant(on, oi) if on == n && oi == i => true,
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

    /// Shorten the lifetime of the event
    pub fn to_self(&self) -> Event<'_> {
        match self {
            Event::OwnedStr(s) => Event::Str(s),
            Event::Str(s) => Event::Str(s),
            Event::Variant(n, i) => Event::Variant(n, *i),
            Event::OwnedVariant(n, i) => Event::Variant(n, *i),
            Event::StartSequence => Event::StartSequence,
            Event::EndSequence => Event::EndSequence,
            Event::StartStruct => Event::StartStruct,
            Event::EndStruct => Event::EndStruct,
            Event::StartTuple => Event::StartTuple,
            Event::EndTuple => Event::EndTuple,
            Event::StartMap => Event::StartMap,
            Event::EndMap => Event::EndMap,
            Event::Some => Event::Some,
            Event::Default => Event::Default,
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
        }
    }

    /// Increase the lifetime of the event to static by cloning any borrowed strings
    ///
    pub fn to_static(&self) -> Event<'static> {
        match self {
            &Event::Str(s) => Event::OwnedStr(s.to_owned()),
            Event::OwnedStr(v) => Event::OwnedStr(v.clone()),
            &Event::Variant(n, i) => Event::OwnedVariant(n.to_owned(), i),
            Event::OwnedVariant(n, i) => Event::OwnedVariant(n.clone(), *i),
            Event::StartSequence => Event::StartSequence,
            Event::EndSequence => Event::EndSequence,
            Event::StartStruct => Event::StartStruct,
            Event::EndStruct => Event::EndStruct,
            Event::StartTuple => Event::StartTuple,
            Event::EndTuple => Event::EndTuple,
            Event::StartMap => Event::StartMap,
            Event::EndMap => Event::EndMap,
            Event::Some => Event::Some,
            Event::Default => Event::Default,
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
        }
    }

    /// Test whether the event increases the nesting level
    pub fn is_start(&self) -> bool {
        matches!(
            self,
            Event::StartSequence | Event::StartStruct | Event::StartMap | Event::StartTuple
        )
    }

    /// Test whether the event decreases the nesting level
    pub fn is_end(&self) -> bool {
        matches!(
            self,
            Event::EndSequence | Event::EndStruct | Event::EndMap | Event::EndTuple
        )
    }

    /// Test whether the event encodes a primitive value
    ///
    /// Note: `Null`, `Default` are not considered a primitive value. Use
    /// [is_value][Event::is_value] to include them.
    pub fn is_primitive(&self) -> bool {
        matches!(
            self,
            Event::Bool(_)
                | Event::Str(_)
                | Event::OwnedStr(_)
                | Event::I8(_)
                | Event::I16(_)
                | Event::I32(_)
                | Event::I64(_)
                | Event::U8(_)
                | Event::U16(_)
                | Event::U32(_)
                | Event::U64(_)
                | Event::F32(_)
                | Event::F64(_)
        )
    }

    /// Test whether the event encodes a self-contained value
    pub fn is_value(&self) -> bool {
        self.is_primitive() || matches!(self, Event::Null | Event::Default)
    }

    /// Test whether the event modifies the following value
    pub fn is_marker(&self) -> bool {
        matches!(self, Event::Some | Event::Variant(_, _))
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

macro_rules! event_implement_try_from_from_event {
    ($ty:ty, $($variant:ident),*) => {
        impl<'a> TryFrom<Event<'a>> for $ty {
            type Error = Error;
            fn try_from(val: Event<'_>) -> Result<$ty> {
                match val {
                    $(Event::$variant(val) => Ok(val.try_into()?),)*

                    // TODO: improve error message
                    event => fail!("Invalid conversion from {} to {}", event, stringify!($ty)),
                }
            }
        }
    };
}

event_implement_try_from_from_event!(bool, Bool);

event_implement_try_from_from_event!(i8, U8, U16, U32, U64, I8, I16, I32, I64, Bool);
event_implement_try_from_from_event!(i16, U8, U16, U32, U64, I8, I16, I32, I64, Bool);
event_implement_try_from_from_event!(i32, U8, U16, U32, U64, I8, I16, I32, I64, Bool);
event_implement_try_from_from_event!(i64, U8, U16, U32, U64, I8, I16, I32, I64, Bool);

event_implement_try_from_from_event!(u8, U8, U16, U32, U64, I8, I16, I32, I64, Bool);
event_implement_try_from_from_event!(u16, U8, U16, U32, U64, I8, I16, I32, I64, Bool);
event_implement_try_from_from_event!(u32, U8, U16, U32, U64, I8, I16, I32, I64, Bool);
event_implement_try_from_from_event!(u64, U8, U16, U32, U64, I8, I16, I32, I64, Bool);

event_implement_try_from_from_event!(f32, F32);
event_implement_try_from_from_event!(f64, F32, F64);

event_implement_try_from_from_event!(String, Str, OwnedStr);
