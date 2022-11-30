use arrow2::types::f16;

use crate::{
    base::{error::fail, Event},
    Error, Result,
};

impl<'a> TryFrom<Event<'a>> for f16 {
    type Error = Error;

    fn try_from(value: Event<'a>) -> Result<Self> {
        match value {
            Event::F32(value) => Ok(f16::from_f32(value)),
            ev => fail!("Cannot convert {ev} to f16"),
        }
    }
}

impl<'a> From<f16> for Event<'a> {
    fn from(value: f16) -> Self {
        Event::F32(value.to_f32())
    }
}
