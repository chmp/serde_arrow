use serde_json::Value;

use crate::{
    internal::{
        schema::{SchemaLike, SerdeArrowSchema},
        testing::assert_error_contains,
        utils::Item,
    },
    schema::TracingOptions,
};

#[test]
fn example() {
    // NOTE: Value cannot be traced with from_type, as it is not self-describing
    let res = SerdeArrowSchema::from_type::<Item<Vec<Value>>>(TracingOptions::default());
    assert_error_contains(&res, "path: \"$.item.element\"");
    assert_error_contains(&res, "tracer_type: \"Unknown\"");
}
