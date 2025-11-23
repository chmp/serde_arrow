use marrow::datatypes::{DataType, Field};
use serde_json::json;

use crate::internal::{
    schema::{SchemaLike, SerdeArrowSchema},
    testing::assert_error_contains,
};

#[test]
fn extra_attributes_trailing() {
    let schema = SerdeArrowSchema::from_value(json!({
        "fields": [
            {"name": "foo", "data_type": "F32"},
        ],
        "trailing": null,
    }))
    .unwrap();
    assert_eq!(
        schema.fields,
        vec![Field {
            name: String::from("foo"),
            data_type: DataType::Float32,
            nullable: false,
            metadata: Default::default(),
        }]
    );
}

#[test]
fn extra_attributes_leading() {
    let schema = SerdeArrowSchema::from_value(json!({
        "leading": null,
        "fields": [
            {"name": "foo", "data_type": "F32"},
        ],
    }))
    .unwrap();
    assert_eq!(
        schema.fields,
        vec![Field {
            name: String::from("foo"),
            data_type: DataType::Float32,
            nullable: false,
            metadata: Default::default(),
        }]
    );
}

#[test]
fn invalid_top_level() {
    let err = SerdeArrowSchema::from_value(json!(true)).expect_err("Expected error");
    let err = err.to_string();

    println!("Actual error: {err}");
    assert!(err.contains("expected a sequence of fields or a struct with key"));
}

#[test]
fn list_missing_dat_tpye() {
    let err = SerdeArrowSchema::from_value(json!([
        {"name": "foo"},
    ]))
    .expect_err("Expected error");
    let err = err.to_string();

    println!("Actual error: {err}");
    assert!(err.contains("missing field `data_type"));
}

#[test]
fn struct_missing_fields() {
    let err = SerdeArrowSchema::from_value(json!({})).expect_err("Expected error");
    let err = err.to_string();

    println!("Actual error: {err}");
    assert!(err.contains("missing field `fields`"));
}

#[test]
fn struct_missing_data_type() {
    let err = SerdeArrowSchema::from_value(json!({
        "fields": [{"name": "foo"}]
    }))
    .expect_err("Expected error");
    let err = err.to_string();

    println!("Actual error: {err}");
    assert!(err.contains("missing field `data_type"));
}

#[test]
fn time64_type_invalid_units() {
    // Note: the arrow docs state: that the time unit "[m]ust be either
    // microseconds or nanoseconds."

    assert_error_contains(
        &SerdeArrowSchema::from_value(json!([{
            "name": "item",
            "data_type": "Time64(Millisecond)",
        }])),
        "Error: Time64 field must have Microsecond or Nanosecond unit",
    );
    assert_error_contains(
        &SerdeArrowSchema::from_value(json!([{
            "name": "item",
            "data_type": "Time64(Second)",
        }])),
        "Error: Time64 field must have Microsecond or Nanosecond unit",
    );

    assert_error_contains(
        &SerdeArrowSchema::from_value(json!([{
            "name": "item",
            "data_type": "Time32(Microsecond)",
        }])),
        "Error: Time32 field must have Second or Millisecond unit",
    );
    assert_error_contains(
        &SerdeArrowSchema::from_value(json!([{
            "name": "item",
            "data_type": "Time32(Nanosecond)",
        }])),
        "Error: Time32 field must have Second or Millisecond unit",
    );
}
