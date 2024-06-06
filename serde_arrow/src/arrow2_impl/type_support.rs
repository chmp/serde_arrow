use crate::internal::error::Error;

impl From<crate::_impl::arrow2::error::Error> for Error {
    fn from(err: crate::_impl::arrow2::error::Error) -> Error {
        Self::custom_from(format!("arrow2::Error: {err}"), err)
    }
}
