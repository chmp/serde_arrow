use crate::internal::error::Error;

#[test]
#[allow(clippy::bool_assert_comparison)]
fn backtrace_on_debug() {
    let err = Error::custom(String::from("foo bar"));

    // NOTE: the exact message depends on the ability of Rust to capture a backtrace
    assert_eq!(format!("{}", err).contains("Backtrace"), false);
    assert_eq!(format!("{:?}", err).contains("Backtrace"), true);
}
