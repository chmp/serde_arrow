use std::{
    backtrace::{Backtrace, BacktraceStatus},
    collections::BTreeMap,
    convert::Infallible,
};

pub fn set_default(
    annotations: &mut BTreeMap<String, String>,
    key: &str,
    value: impl std::fmt::Display,
) {
    if !annotations.contains_key(key) {
        annotations.insert(String::from(key), value.to_string());
    }
}

pub fn prepend(
    annotations: &mut BTreeMap<String, String>,
    key: &str,
    value: impl std::fmt::Display,
) {
    if let Some(prev) = annotations.get_mut(key) {
        *prev = format!("{}.{}", value, prev);
    } else {
        annotations.insert(String::from(key), value.to_string());
    }
}

pub struct FieldName<'a>(pub &'a str);

impl std::fmt::Display for FieldName<'_> {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        if !self.0.is_empty() {
            std::fmt::Display::fmt(self.0, f)
        } else {
            write!(f, "<empty>")
        }
    }
}

/// Execute a faillible function and return the result
///
/// This function is mostly useful to add annotations to a complex block of operations
pub fn try_<T>(func: impl FnOnce() -> Result<T>) -> Result<T> {
    func()
}

/// An object that offers additional context to an error
pub trait Context {
    fn annotate(&self, annotations: &mut BTreeMap<String, String>);
}

impl Context for BTreeMap<String, String> {
    fn annotate(&self, annotations: &mut BTreeMap<String, String>) {
        for (k, v) in self {
            if !annotations.contains_key(k) {
                annotations.insert(k.to_owned(), v.to_owned());
            }
        }
    }
}

/// Helpers to attach the metadata associated with a context to an error
pub trait ContextSupport {
    type Output;

    fn ctx<C: Context>(self, context: &C) -> Self::Output;
}

impl<T, E: Into<Error>> ContextSupport for Result<T, E> {
    type Output = Result<T, Error>;

    fn ctx<C: Context>(self, context: &C) -> Self::Output {
        match self {
            Ok(value) => Ok(value),
            Err(err) => Err(err.ctx(context)),
        }
    }
}

impl<E: Into<Error>> ContextSupport for E {
    type Output = Error;

    fn ctx<C: Context>(self, context: &C) -> Self::Output {
        let Error::Custom(mut error) = self.into();
        context.annotate(&mut error.0.annotations);
        Error::Custom(error)
    }
}

/// A Result type that defaults to `serde_arrow`'s [Error] type
///
pub type Result<T, E = Error> = std::result::Result<T, E>;

/// Common errors during `serde_arrow`'s usage
///
/// At the moment only a generic string error is supported, but it is planned to offer concrete
/// types to match against.
///
/// The error carries a backtrace if `RUST_BACKTRACE=1`, see [`std::backtrace`] for details. This
/// backtrace is included when printing the error. If the error is caused by another error, that
/// error can be retrieved with [`source()`][std::error::Error::source].
///
/// # Display representation
///
/// This error type follows anyhow's display representation: when printed with display format (`{}`)
/// (or converted to string) the error does not include a backtrace. Use the debug format (`{:?}`)
/// to include the backtrace information.
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

    pub(crate) fn modify_message<F: FnOnce(&mut String)>(&mut self, func: F) {
        let Error::Custom(this) = self;
        let inner = this.0.as_mut();
        func(&mut inner.message);
    }
}

#[derive(PartialEq)]
pub struct CustomError(pub(crate) Box<CustomErrorImpl>);

pub struct CustomErrorImpl {
    pub(crate) message: String,
    pub(crate) backtrace: Backtrace,
    pub(crate) cause: Option<Box<dyn std::error::Error + Send + Sync + 'static>>,
    pub(crate) annotations: BTreeMap<String, String>,
}

impl std::cmp::PartialEq for CustomErrorImpl {
    fn eq(&self, other: &Self) -> bool {
        self.message == other.message && self.annotations == other.annotations
    }
}

impl std::fmt::Debug for Error {
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

impl std::fmt::Display for Error {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(
            f,
            "Error: {msg}{annotations}",
            msg = self.message(),
            annotations = AnnotationsDisplay(self.annotations()),
        )
    }
}

struct AnnotationsDisplay<'a>(Option<&'a BTreeMap<String, String>>);

impl std::fmt::Display for AnnotationsDisplay<'_> {
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

impl std::fmt::Display for BacktraceDisplay<'_> {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self.0.status() {
            BacktraceStatus::Captured => write!(f, "Backtrace:\n{bt}", bt=self.0),
            BacktraceStatus::Disabled => write!(f, "Backtrace not captured; set the `RUST_BACKTRACE=1` env variable to enable"),
            _ => write!(f, "Backtrace not captured: most likely backtraces are not supported on the current platform"),
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

macro_rules! fail {
    // TODO: Remove context support. Context should only be added add specified recursion points in
    // serializers or deserializers making this macro form obsolete
    (in $context:expr, $($tt:tt)*) => {
        {
            #[allow(unused)]
            use $crate::internal::error::Context;
            let $crate::internal::error::Error::Custom(mut err) = $crate::internal::error::Error::custom(format!($($tt)*));
            $context.annotate(&mut err.0.annotations);
            return Err($crate::internal::error::Error::Custom(err));
        }
    };
    ($($tt:tt)*) => {
        return Err($crate::internal::error::Error::custom(format!($($tt)*)))
    };
}

pub(crate) use fail;

impl From<marrow::error::MarrowError> for Error {
    fn from(err: marrow::error::MarrowError) -> Self {
        Self::custom_from(format!("marrow::error::MarrowError: {err}"), err)
    }
}

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

// use Display to not match PanicOnErrorError itself, use Debug for printing to include stacktrace
impl<E: std::fmt::Display + std::fmt::Debug> From<E> for PanicOnErrorError {
    fn from(value: E) -> Self {
        panic!("{value:?}");
    }
}

#[test]
fn error_can_be_converted_to_anyhow() {
    fn func() -> anyhow::Result<()> {
        Err(Error::custom("dummy".to_string()))?;
        Ok(())
    }
    assert!(func().is_err());
}

#[allow(unused)]
const _: () = {
    trait AssertSendSync: Send + Sync {}
    impl AssertSendSync for Error {}
    impl<T: Send + Sync> AssertSendSync for Result<T> {}
};
