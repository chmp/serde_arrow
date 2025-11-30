use serde_json::json;

use crate::internal::{
    array_builder::ArrayBuilder,
    error::PanicOnError,
    schema::{SchemaLike, SerdeArrowSchema},
    testing::assert_error_contains,
};

#[test]
fn int_nested() -> PanicOnError<()> {
    let schema = SerdeArrowSchema::from_value(json!([
        {
            "name": "nested",
            "data_type": "Struct",
            "children": [
                {"name": "field", "data_type": "U32"},
            ],
        },
    ]))?;

    let mut array_builder = ArrayBuilder::new(schema)?;
    array_builder
        .push(json!({"nested": {"field": 32}}))
        .unwrap();

    let err = array_builder
        .push(json!({"nested": {"field": null}}))
        .unwrap_err();
    assert_error_contains(&err, "field: \"$.nested.field\"");

    Ok(())
}

#[test]
fn int_top_level() -> PanicOnError<()> {
    let schema = SerdeArrowSchema::from_value(json!([
        {"name": "field", "data_type": "U32"},
    ]))?;

    let mut array_builder = ArrayBuilder::new(schema)?;
    array_builder.push(json!({"field": 32})).unwrap();

    let err = array_builder.push(json!({"field": null})).unwrap_err();
    assert_error_contains(&err, "field: \"$.field\"");

    Ok(())
}

#[test]
fn struct_nested() -> PanicOnError<()> {
    let schema = SerdeArrowSchema::from_value(json!([
        {
            "name": "nested",
            "data_type": "Struct",
            "children": [
                {"name": "field", "data_type": "Struct", "children": []},
            ],
        },
    ]))?;

    let mut array_builder = ArrayBuilder::new(schema)?;
    array_builder
        .push(json!({"nested": {"field": {}}}))
        .unwrap();

    let err = array_builder
        .push(json!({"nested": {"field": null}}))
        .unwrap_err();
    assert_error_contains(&err, "field: \"$.nested.field\"");

    Ok(())
}

#[test]
fn struct_top_level() -> PanicOnError<()> {
    let schema = SerdeArrowSchema::from_value(json!([
        {"name": "field", "data_type": "Struct", "children": []},
    ]))?;

    let mut array_builder = ArrayBuilder::new(schema)?;
    array_builder.push(json!({"field": {}})).unwrap();

    let err = array_builder.push(json!({"field": null})).unwrap_err();
    assert_error_contains(&err, "field: \"$.field\"");

    Ok(())
}
