use crate::internal::error::{Error, ErrorKind};

#[test]
#[allow(clippy::bool_assert_comparison)]
fn backtrace_on_debug() {
    let err = Error::new(ErrorKind::Custom, String::from("foo bar"));

    // NOTE: the exact message depends on the ability of Rust to capture a backtrace
    assert!(!format!("{}", err).contains("Backtrace"));
    assert!(format!("{:?}", err).contains("Backtrace"));
}
