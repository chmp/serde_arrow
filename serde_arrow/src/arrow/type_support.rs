use crate::_impl::arrow::error::ArrowError;

use crate::internal::error::Error;

impl From<ArrowError> for Error {
    fn from(err: ArrowError) -> Self {
        Self::custom(err.to_string())
    }
}
