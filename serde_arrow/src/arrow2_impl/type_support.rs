use crate::{
    _impl::arrow2::types::f16,
    internal::{error::Error, event::Event},
};

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
