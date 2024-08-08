use std::{
    backtrace::{Backtrace, BacktraceStatus},
    convert::Infallible,
};

/// A Result type that defaults to `serde_arrow`'s [Error] type
///
pub type Result<T, E = Error> = std::result::Result<T, E>;

/// Common errors during `serde_arrow`'s usage
///
/// At the moment only a generic string error is supported, but it is planned to
/// offer concrete types to match against.
///
/// The error carries a backtrace if `RUST_BACKTRACE=1`, see [`std::backtrace`]
/// for details. This backtrace is included when printing the error. If the
/// error is caused by another error, that error can be retrieved with
/// [`source()`][std::error::Error::source].
///
#[derive(PartialEq)]
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

impl std::cmp::PartialEq for CustomError {
    fn eq(&self, other: &Self) -> bool {
        self.message == other.message
    }
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
                "Error: {msg}\n{bt}",
                msg = e.message,
                bt = BacktraceDisplay(&e.backtrace),
            ),
        }
    }
}

struct BacktraceDisplay<'a>(&'a Backtrace);

impl<'a> std::fmt::Display for BacktraceDisplay<'a> {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self.0.status() {
            BacktraceStatus::Captured => write!(f, "Backtrace:\n{bt}", bt=self.0),
            BacktraceStatus::Disabled => write!(f, "No backtrace captured. Set the `RUST_BACKTRACE=1` env variable to enable."),
            _ => write!(f, "No backtrace captured. Most likely backtraces are not supported on the current platform."),
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
        $crate::internal::error::Error::custom(format!($($tt)*))
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

impl From<std::char::CharTryFromError> for Error {
    fn from(err: std::char::CharTryFromError) -> Error {
        Self::custom_from(format!("CharTryFromError: {err}"), err)
    }
}

impl From<std::char::TryFromCharError> for Error {
    fn from(err: std::char::TryFromCharError) -> Error {
        Self::custom_from(format!("TryFromCharError: {err}"), err)
    }
}

impl From<std::num::TryFromIntError> for Error {
    fn from(err: std::num::TryFromIntError) -> Error {
        Self::custom_from(format!("TryFromIntError: {err}"), err)
    }
}

impl From<std::num::ParseIntError> for Error {
    fn from(err: std::num::ParseIntError) -> Self {
        Self::custom_from(format!("ParseIntError: {err}"), err)
    }
}

impl From<std::fmt::Error> for Error {
    fn from(err: std::fmt::Error) -> Self {
        Self::custom_from(format!("std::fmt::Error: {err}"), err)
    }
}

impl From<std::str::Utf8Error> for Error {
    fn from(err: std::str::Utf8Error) -> Self {
        Self::custom_from(format!("std::str::Utf8Error: {err}"), err)
    }
}

impl From<Infallible> for Error {
    fn from(_: Infallible) -> Self {
        unreachable!()
    }
}

impl From<bytemuck::PodCastError> for Error {
    fn from(err: bytemuck::PodCastError) -> Self {
        Self::custom(format!("bytemuck::PodCastError: {err}"))
    }
}

pub type PanicOnError<T> = std::result::Result<T, PanicOnErrorError>;

/// An error type for testing, that panics once an error is converted
#[derive(Debug)]
pub struct PanicOnErrorError;

impl<E: std::fmt::Display> From<E> for PanicOnErrorError {
    fn from(value: E) -> Self {
        panic!("{value}");
    }
}

#[test]
fn error_can_be_converted_to_anyhow() {
    fn func() -> anyhow::Result<()> {
        Err(error!("dummy"))?;
        Ok(())
    }
    assert!(func().is_err());
}
