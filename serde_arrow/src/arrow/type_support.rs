use arrow_schema_36::ArrowError;

use crate::Error;

impl From<ArrowError> for Error {
    fn from(err: ArrowError) -> Self {
        Self::custom(err.to_string())
    }
}
