use chrono::{serde::ts_microseconds, DateTime, Utc};
use serde::{Deserialize, Serialize};
use serde_json::json;

use crate::internal::{
    error::PanicOnError,
    schema::{SchemaLike, SerdeArrowSchema, TracingOptions},
};

/// https://github.com/chmp/serde_arrow/issues/187
#[test]
fn example_issue_187() -> PanicOnError<()> {
    #[derive(Debug, Serialize, Deserialize)]
    struct Example {
        #[serde(with = "ts_microseconds")]
        pub expiry: DateTime<Utc>,
    }

    let options = TracingOptions::default().overwrite(
        "expiry",
        json!({"name": "expiry", "data_type": "Timestamp(Microsecond, None)"}),
    )?;
    let actual = SerdeArrowSchema::from_type::<Example>(options)?;

    let expected = SerdeArrowSchema::from_value(&json!([
        {"name": "expiry", "data_type": "Timestamp(Microsecond, None)"}
    ]))?;

    assert_eq!(actual, expected);
    Ok(())
}

#[test]
fn example_nested_overwrites_vec() -> PanicOnError<()> {
    #[derive(Debug, Serialize, Deserialize)]
    struct Example {
        pub date_times: Vec<i64>,
    }

    let options = TracingOptions::default().overwrite(
        "date_times.element",
        json!({"name": "element", "data_type": "Timestamp(Microsecond, None)"}),
    )?;
    let actual = SerdeArrowSchema::from_type::<Example>(options)?;

    let expected = SerdeArrowSchema::from_value(&json!([
        {
            "name": "date_times",
            "data_type": "LargeList",
            "children": [
                {"name": "element", "data_type": "Timestamp(Microsecond, None)"},
            ],
        }
    ]))?;

    assert_eq!(actual, expected);
    Ok(())
}

#[test]
fn example_nested_overwrites_structs() -> PanicOnError<()> {
    #[derive(Debug, Serialize, Deserialize)]
    struct Example {
        pub inner: Inner,
    }

    #[derive(Debug, Serialize, Deserialize)]
    struct Inner {
        pub value: i64,
    }

    let options = TracingOptions::default().overwrite(
        "inner.value",
        json!({"name": "value", "data_type": "I32"}),
    )?;
    let actual = SerdeArrowSchema::from_type::<Example>(options)?;

    let expected = SerdeArrowSchema::from_value(&json!([
        {
            "name": "inner",
            "data_type": "Struct",
            "children": [
                {"name": "value", "data_type": "I32"},
            ],
        }
    ]))?;

    assert_eq!(actual, expected);
    Ok(())
}

#[test]
fn example_renames() {
    #[derive(Debug, Serialize, Deserialize)]
    struct Example {
        pub value: i64,
    }

    let actual = SerdeArrowSchema::from_type::<Example>(
        TracingOptions::default()
            .overwrite(
                "value",
                json!({"name": "renamed_value", "data_type": "I64"}),
            )
            .unwrap(),
    )
    .unwrap();
    let expected = SerdeArrowSchema::from_value(&json!([
        {"name": "renamed_value", "data_type": "I64"}
    ]))
    .unwrap();

    assert_eq!(actual, expected);
}

#[test]
fn example_renames_with_arrow_field() {
    use crate::_impl::arrow::datatypes::{DataType, Field};

    #[derive(Debug, Serialize, Deserialize)]
    struct Example {
        pub value: i64,
    }

    let actual = SerdeArrowSchema::from_type::<Example>(
        TracingOptions::default()
            .overwrite(
                "value",
                Field::new("renamed_value", DataType::Int64, false),
            )
            .unwrap(),
    )
    .unwrap();
    let expected = SerdeArrowSchema::from_value(&json!([
        {"name": "renamed_value", "data_type": "I64"}
    ]))
    .unwrap();

    assert_eq!(actual, expected);
}
