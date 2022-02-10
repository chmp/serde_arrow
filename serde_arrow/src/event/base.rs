//! The underlying data format used to interact with serde
//!
#[derive(Debug, PartialEq)]
pub enum Event<'a> {
    StartSequence,
    StartMap,
    Key(&'a str),
    I8(i8),
    I32(i32),
    EndMap,
    EndSequence,
}

impl<'a> std::fmt::Display for Event<'a> {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Event::StartSequence => write!(f, "StartSequence"),
            Event::StartMap => write!(f, "StartMap"),
            Event::Key(_) => write!(f, "Key"),
            Event::I8(_) => write!(f, "I8"),
            Event::I32(_) => write!(f, "I32"),
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

event_implement_simple_from!(i8, I8);
event_implement_simple_from!(i32, I32);
