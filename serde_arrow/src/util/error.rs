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
        Self::Custom(msg.to_string())
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
        return Err($crate::error!($($tt)*));
    };
}

impl From<arrow::error::ArrowError> for Error {
    fn from(error: arrow::error::ArrowError) -> Error {
        Error::Custom(error.to_string())
    }
}

impl From<chrono::format::ParseError> for Error {
    fn from(error: chrono::format::ParseError) -> Self {
        Self::Custom(error.to_string())
    }
}
