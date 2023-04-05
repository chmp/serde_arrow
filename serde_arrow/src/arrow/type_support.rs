use crate::_impl::arrow::schema::ArrowError;

use crate::Error;

impl From<ArrowError> for Error {
    fn from(err: ArrowError) -> Self {
        Self::custom(err.to_string())
    }
}
