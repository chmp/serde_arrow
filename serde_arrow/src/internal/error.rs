use std::{backtrace::Backtrace, convert::Infallible};

/// A Result type that defaults to `serde_arrow`'s [Error] type
///
pub type Result<T, E = Error> = std::result::Result<T, E>;

/// Common errors during `serde_arrow`'s usage
///
/// At the moment only a generic string error is supported, but it is planned to
/// offer concrete types to match against.
///
/// The error carries a backtrace if `RUST_BACKTRACE=1`, see [std::backtrace]
/// for details. This backtrace is included when printing the error. If the
/// error is caused by another error, that error can be retrieved with the
/// [source][std::error::Error::source] function.
///
#[non_exhaustive]
pub enum Error {
    Custom(CustomError),
}

impl Error {
    pub fn custom(message: String) -> Self {
        Self::Custom(CustomError {
            message,
            backtrace: Backtrace::capture(),
            cause: None,
        })
    }

    pub fn custom_from<E: std::error::Error + Send + Sync + 'static>(
        message: String,
        cause: E,
    ) -> Self {
        Self::Custom(CustomError {
            message,
            backtrace: Backtrace::capture(),
            cause: Some(Box::new(cause)),
        })
    }
}

impl Error {
    pub fn message(&self) -> &str {
        match self {
            Self::Custom(err) => &err.message,
        }
    }

    pub fn backtrace(&self) -> &Backtrace {
        match self {
            Self::Custom(err) => &err.backtrace,
        }
    }
}

pub struct CustomError {
    message: String,
    backtrace: Backtrace,
    cause: Option<Box<dyn std::error::Error + Send + Sync + 'static>>,
}

impl std::fmt::Debug for Error {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "<{self}>")
    }
}

impl std::fmt::Display for Error {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Error::Custom(e) => write!(
                f,
                "Error: {msg}\nBacktrace:\n{bt}",
                msg = e.message,
                bt = e.backtrace,
            ),
        }
    }
}

impl std::error::Error for Error {
    fn source(&self) -> Option<&(dyn std::error::Error + 'static)> {
        match self {
            Self::Custom(CustomError {
                cause: Some(err), ..
            }) => Some(err.as_ref()),
            _ => None,
        }
    }
}

impl serde::ser::Error for Error {
    fn custom<T>(msg: T) -> Self
    where
        T: std::fmt::Display,
    {
        Self::custom(format!("serde::ser::Error: {}", msg))
    }
}

impl serde::de::Error for Error {
    fn custom<T>(msg: T) -> Self
    where
        T: std::fmt::Display,
    {
        Self::custom(format!("serde::de::Error: {}", msg))
    }
}

macro_rules! error {
    ($($tt:tt)*) => {
        $crate::Error::custom(format!($($tt)*))
    };
}

pub(crate) use error;

macro_rules! fail {
    ($($tt:tt)*) => {
        return Err($crate::internal::error::error!($($tt)*))
    };
}

pub(crate) use fail;

impl From<chrono::format::ParseError> for Error {
    fn from(err: chrono::format::ParseError) -> Self {
        Self::custom_from(format!("chrono::ParseError: {err}"), err)
    }
}

impl From<std::num::TryFromIntError> for Error {
    fn from(err: std::num::TryFromIntError) -> Error {
        Self::custom_from(format!("arrow2::Error: {err}"), err)
    }
}

impl From<std::fmt::Error> for Error {
    fn from(err: std::fmt::Error) -> Self {
        Self::custom_from(format!("std::fmt::Error: {err}"), err)
    }
}

impl From<Infallible> for Error {
    fn from(_: Infallible) -> Self {
        unreachable!()
    }
}
