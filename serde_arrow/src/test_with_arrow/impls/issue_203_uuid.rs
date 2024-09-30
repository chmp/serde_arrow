use serde_json::json;
use uuid::Uuid;

use crate::{
    internal::testing::assert_error_contains,
    schema::{SchemaLike, SerdeArrowSchema, TracingOptions},
    utils::Item,
};

use super::utils::Test;

#[test]
fn example_as_list() {
    let items = [
        Item(Uuid::new_v4()),
        Item(Uuid::new_v4()),
        Item(Uuid::new_v4()),
    ];

    Test::new()
        .with_schema(json!([{
            "name": "item",
            "data_type": "LargeUtf8",
        }]))
        .trace_schema_from_samples(&items, TracingOptions::default())
        .serialize(&items)
        .deserialize(&items);
}

#[test]
fn trace_from_type_does_not_work() {
    let res = SerdeArrowSchema::from_type::<Item<Uuid>>(TracingOptions::default());
    assert_error_contains(&res, "UUID parsing failed");
    assert_error_contains(&res, "non self describing type");
}
