use serde_json::json;

use crate::internal::schema::{GenericDataType, GenericField};
use crate::schema::{SchemaLike, SerdeArrowSchema};

#[test]
fn extra_attributes_trailing() {
    let schema = SerdeArrowSchema::from_value(&json!({
        "fields": [
            {"name": "foo", "data_type": "F32"},
        ],
        "trailing": null,
    }))
    .unwrap();
    assert_eq!(
        schema.fields,
        vec![GenericField::new("foo", GenericDataType::F32, false)]
    );
}

#[test]
fn extra_attributes_leading() {
    let schema = SerdeArrowSchema::from_value(&json!({
        "leading": null,
        "fields": [
            {"name": "foo", "data_type": "F32"},
        ],
    }))
    .unwrap();
    assert_eq!(
        schema.fields,
        vec![GenericField::new("foo", GenericDataType::F32, false)]
    );
}

#[test]
fn invalid_top_level() {
    let err = SerdeArrowSchema::from_value(&json!(true)).expect_err("Expected error");
    let err = err.to_string();

    println!("Actual error: {err}");
    assert!(err.contains("expected a sequence of fields or a struct with key"));
}

#[test]
fn list_missing_dat_tpye() {
    let err = SerdeArrowSchema::from_value(&json!([
        {"name": "foo"},
    ]))
    .expect_err("Expected error");
    let err = err.to_string();

    println!("Actual error: {err}");
    assert!(err.contains("missing field `data_type"));
}

#[test]
fn struct_missing_fields() {
    let err = SerdeArrowSchema::from_value(&json!({})).expect_err("Expected error");
    let err = err.to_string();

    println!("Actual error: {err}");
    assert!(err.contains("missing field `fields`"));
}

#[test]
fn struct_missing_data_type() {
    let err = SerdeArrowSchema::from_value(&json!({
        "fields": [{"name": "foo"}]
    }))
    .expect_err("Expected error");
    let err = err.to_string();

    println!("Actual error: {err}");
    assert!(err.contains("missing field `data_type"));
}
