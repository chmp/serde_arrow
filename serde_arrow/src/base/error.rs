/// A Result type that defaults to `serde_arrow`'s Error type
///
pub type Result<T, E = Error> = std::result::Result<T, E>;

/// Common errors during `serde_arrow`'s usage
///
/// At the moment
#[derive(Debug)]
pub enum Error {
    Custom(String),
}

impl std::fmt::Display for Error {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Error::Custom(msg) => write!(f, "Error: {}", msg),
        }
    }
}

impl std::error::Error for Error {}

impl serde::ser::Error for Error {
    fn custom<T>(msg: T) -> Self
    where
        T: std::fmt::Display,
    {
        Self::Custom(format!("serde::ser::Error: {}", msg))
    }
}

impl serde::de::Error for Error {
    fn custom<T>(msg: T) -> Self
    where
        T: std::fmt::Display,
    {
        Self::Custom(format!("serde::de::Error: {}", msg))
    }
}

macro_rules! error {
    ($($tt:tt)*) => {
        $crate::Error::Custom(format!($($tt)*))
    };
}
pub(crate) use error;

macro_rules! fail {
    ($($tt:tt)*) => {
        return Err($crate::base::error::error!($($tt)*))
    };
}

pub(crate) use fail;

#[cfg(feature = "arrow2")]
impl From<arrow2::error::Error> for Error {
    fn from(error: arrow2::error::Error) -> Error {
        Error::Custom(format!("arrow2::Error: {error}"))
    }
}

impl From<chrono::format::ParseError> for Error {
    fn from(error: chrono::format::ParseError) -> Self {
        Self::Custom(format!("chrono::ParseError: {error}"))
    }
}

impl From<std::num::TryFromIntError> for Error {
    fn from(error: std::num::TryFromIntError) -> Error {
        Error::Custom(format!("arrow2::Error: {error}"))
    }
}

impl From<std::fmt::Error> for Error {
    fn from(err: std::fmt::Error) -> Self {
        Error::Custom(format!("std::fmt::Error: {err}"))
    }
}
