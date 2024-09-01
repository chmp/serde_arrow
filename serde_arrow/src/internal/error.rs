use std::{
    backtrace::{Backtrace, BacktraceStatus},
    collections::BTreeMap,
    convert::Infallible,
};

pub trait Context {
    fn annotations(&self) -> BTreeMap<String, String>;
}

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

/// Error creation
impl Error {
    pub fn custom(message: String) -> Self {
        Self::Custom(CustomError(Box::new(CustomErrorImpl {
            message,
            backtrace: Backtrace::capture(),
            cause: None,
            annotations: BTreeMap::new(),
        })))
    }

    pub fn custom_from<E: std::error::Error + Send + Sync + 'static>(
        message: String,
        cause: E,
    ) -> Self {
        Self::Custom(CustomError(Box::new(CustomErrorImpl {
            message,
            backtrace: Backtrace::capture(),
            cause: Some(Box::new(cause)),
            annotations: BTreeMap::new(),
        })))
    }

    pub(crate) fn empty() -> Self {
        Self::Custom(CustomError(Box::new(CustomErrorImpl {
            message: String::new(),
            backtrace: Backtrace::disabled(),
            cause: None,
            annotations: BTreeMap::new(),
        })))
    }
}

impl Error {
    /// Call the function with a mutable reference to this errors annotations, if the error was not
    /// annotated before
    pub(crate) fn annotate_unannotated<F: FnOnce(&mut BTreeMap<String, String>)>(
        self,
        func: F,
    ) -> Self {
        let Self::Custom(mut this) = self;
        if this.0.annotations.is_empty() {
            func(&mut this.0.annotations);
        }
        Self::Custom(this)
    }

    pub(crate) fn with_annotations(self, annotations: BTreeMap<String, String>) -> Self {
        let Self::Custom(mut this) = self;
        this.0.annotations = annotations;
        Self::Custom(this)
    }
}

/// Access information about the error
impl Error {
    pub fn message(&self) -> &str {
        match self {
            Self::Custom(err) => &err.0.message,
        }
    }

    pub fn backtrace(&self) -> &Backtrace {
        match self {
            Self::Custom(err) => &err.0.backtrace,
        }
    }

    /// Get a reference to the annotations of this error
    pub(crate) fn annotations(&self) -> Option<&BTreeMap<String, String>> {
        match self {
            Self::Custom(err) => Some(&err.0.annotations),
        }
    }
}

#[derive(PartialEq)]
pub struct CustomError(pub(crate) Box<CustomErrorImpl>);

pub struct CustomErrorImpl {
    message: String,
    backtrace: Backtrace,
    cause: Option<Box<dyn std::error::Error + Send + Sync + 'static>>,
    pub(crate) annotations: BTreeMap<String, String>,
}

impl std::cmp::PartialEq for CustomErrorImpl {
    fn eq(&self, other: &Self) -> bool {
        self.message == other.message && self.annotations == other.annotations
    }
}

impl std::fmt::Debug for Error {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "<{self}>")
    }
}

impl std::fmt::Display for Error {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(
            f,
            "Error: {msg}{annotations}\n{bt}",
            msg = self.message(),
            annotations = AnnotationsDisplay(self.annotations()),
            bt = BacktraceDisplay(self.backtrace()),
        )
    }
}

struct AnnotationsDisplay<'a>(Option<&'a BTreeMap<String, String>>);

impl<'a> std::fmt::Display for AnnotationsDisplay<'a> {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        let Some(annotations) = self.0 else {
            return Ok(());
        };
        if annotations.is_empty() {
            return Ok(());
        }

        write!(f, " (")?;
        for (idx, (key, value)) in annotations.iter().enumerate() {
            if idx != 0 {
                write!(f, ", ")?;
            }
            write!(f, "{key}: {value:?}")?;
        }
        write!(f, ")")
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
        let Self::Custom(this) = self;
        match this.0.cause.as_ref() {
            Some(cause) => Some(cause.as_ref()),
            None => None,
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
    (in $context:expr, $($tt:tt)*) => {
        {
            #[allow(unused)]
            use $crate::internal::error::Context;
            let annotations = $context.annotations();
            return Err($crate::internal::error::error!($($tt)*).with_annotations(annotations))
        }
    };
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
        // Note: bytemuck::PodCastError does not implement std::error::Error
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

const _: () = {
    trait AssertSendSync: Send + Sync {}
    impl AssertSendSync for Error {}
    impl<T: Send + Sync> AssertSendSync for Result<T> {}
};
