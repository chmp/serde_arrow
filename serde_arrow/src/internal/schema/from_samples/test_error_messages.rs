use serde::Serialize;

use crate::internal::{
    schema::{SchemaLike, SerdeArrowSchema, TracingOptions},
    testing::assert_error_contains,
};

#[test]
fn outer_struct() {
    let res = SerdeArrowSchema::from_samples([1_u32, 2_u32, 3_u32], TracingOptions::default());
    assert_error_contains(
        &res,
        "Only struct-like types are supported as root types in schema tracing.",
    );
    assert_error_contains(&res, "Consider using the `Items` wrapper,");
}

/// See: https://github.com/chmp/serde_arrow/issues/97
#[test]
fn outer_sequence_issue_97() {
    use serde::Serialize;

    #[derive(Debug, Serialize)]
    pub struct A {
        pub b: String,
        pub k: f64,
    }
    let b = A {
        b: String::from("Test"),
        k: 100.0,
    };

    let res = SerdeArrowSchema::from_samples(&b, TracingOptions::default());
    assert_error_contains(&res, "Cannot trace non-sequences with `from_samples`");
    assert_error_contains(&res, "consider wrapping the argument in an array");
}

#[test]
fn enums_without_data() {
    #[derive(Debug, Serialize)]
    pub enum E {
        A,
        B,
    }

    let res = SerdeArrowSchema::from_samples(&[E::A, E::B], TracingOptions::default());
    assert_error_contains(&res, "by setting `enums_without_data_as_strings` to `true`");
}
