use crate::{
    internal::testing::assert_error_contains,
    schema::{SchemaLike, SerdeArrowSchema, TracingOptions},
    utils::Item,
};

#[test]
fn non_sequence() {
    let err = SerdeArrowSchema::from_samples(42, TracingOptions::default()).unwrap_err();
    assert_error_contains(&err, "Cannot trace non-sequences with `from_samples`");
    assert_error_contains(&err, "path: \"$\"");
}

#[test]
fn incompatible_primitives() {
    let err =
        SerdeArrowSchema::from_samples((Item(42_u32), Item("foo bar")), TracingOptions::default())
            .unwrap_err();
    assert_error_contains(&err, "path: \"$.item\"");
}

#[test]
fn number_coercion() {
    let err =
        SerdeArrowSchema::from_samples((&32.0_f32, 42_u64), TracingOptions::default()).unwrap_err();
    assert_error_contains(
        &err,
        "consider setting `coerce_numbers` to `true` to coerce different numeric types.",
    );
}
