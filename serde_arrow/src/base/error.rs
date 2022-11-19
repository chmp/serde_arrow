pub type Result<T, E = Error> = std::result::Result<T, E>;

/// Errors during conversion or tracing
///
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

#[macro_export]
macro_rules! error {
    ($msg:literal) => {
        $crate::Error::Custom(format!($msg))
    };
    ($msg:literal, $($item:expr),*) => {
        $crate::Error::Custom(format!($msg, $($item),*))
    };
}

#[macro_export]
macro_rules! fail {
    ($($tt:tt)*) => {
        return Err($crate::error!($($tt)*))
    };
}

#[cfg(feature = "arrow")]
impl From<arrow::error::ArrowError> for Error {
    fn from(error: arrow::error::ArrowError) -> Error {
        Error::Custom(format!("arrow::ArrowError: {error}"))
    }
}

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
