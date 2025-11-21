use crate::{
    internal::testing::assert_error_contains,
    schema::{SchemaLike, SerdeArrowSchema, TracingOptions},
    utils::Item,
};

#[test]
fn non_sequence() {
    let res = SerdeArrowSchema::from_samples(42, TracingOptions::default());
    assert_error_contains(&res, "Cannot trace non-sequences with `from_samples`");
    assert_error_contains(&res, "path: \"$\"");
}

#[test]
fn incompatible_primitives() {
    let res =
        SerdeArrowSchema::from_samples((Item(42_u32), Item("foo bar")), TracingOptions::default());
    assert_error_contains(&res, "path: \"$.item\"");
}

#[test]
fn number_coercion() {
    let res = SerdeArrowSchema::from_samples((&32.0_f32, 42_u64), TracingOptions::default());
    assert_error_contains(
        &res,
        "consider setting `coerce_numbers` to `true` to coerce different numeric types.",
    );
}
