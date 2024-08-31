use serde_json::json;

use crate::internal::{
    array_builder::ArrayBuilder,
    error::PanicOnError,
    schema::{SchemaLike, SerdeArrowSchema},
    testing::assert_error_contains,
};

#[test]
fn push_validity_issue_202() -> PanicOnError<()> {
    let schema = SerdeArrowSchema::from_value(&json!([
        {
            "name": "nested",
            "data_type": "Struct",
            "children": [
                {"name": "field", "data_type": "U32"},
            ],
        },
    ]))?;

    let mut array_builder = ArrayBuilder::new(schema)?;
    let res = array_builder.push(&json!({"nested": {"field": 32}}));
    assert_eq!(res, Ok(()));

    let res = array_builder.push(&json!({"nested": {"field": null}}));
    assert_error_contains(&res, "field: \"$.nested.field\"");

    Ok(())
}
