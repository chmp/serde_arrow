use crate::{
    _impl::arrow2::types::f16,
    internal::{
        conversions::ToBytes,
        error::{fail, Error, Result},
        event::Event,
    },
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

impl From<crate::_impl::arrow2::error::Error> for Error {
    fn from(err: crate::_impl::arrow2::error::Error) -> Error {
        Self::custom_from(format!("arrow2::Error: {err}"), err)
    }
}

impl ToBytes for f16 {
    type Bytes = u16;

    fn to_bytes(self) -> Self::Bytes {
        self.to_bits()
    }

    fn from_bytes(val: Self::Bytes) -> Self {
        Self::from_bits(val)
    }
}
