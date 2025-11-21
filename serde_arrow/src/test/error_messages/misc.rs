use crate::internal::error::Error;

#[test]
fn backtrace_on_debug() {
    let err = Error::custom(String::from("foo bar"));

    // NOTE: the exact message depends on the ability of Rust to capture a backtrace
    assert!(!format!("{}", err).contains("Backtrace"));
    assert!(format!("{:?}", err).contains("Backtrace"));
}
