use crate::{internal::testing::assert_error_contains, schema::{SchemaLike, SerdeArrowSchema, TracingOptions}};


#[test]
fn non_sequence() {
    let res = SerdeArrowSchema::from_samples(&42, TracingOptions::default());
    assert_error_contains(&res, "Cannot trace non-sequences with `from_samples`");
    assert_error_contains(&res, "path: \"$\"");
}
