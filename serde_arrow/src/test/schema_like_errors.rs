use serde_json::json;

use crate::schema::{SchemaLike, SerdeArrowSchema};

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
