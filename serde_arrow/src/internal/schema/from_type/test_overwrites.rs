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
        "$.expiry",
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
fn example_nested_overwrites() -> PanicOnError<()> {
    #[derive(Debug, Serialize, Deserialize)]
    struct Example {
        pub date_times: Vec<i64>,
    }

    let options = TracingOptions::default().overwrite(
        "$.date_times.element",
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
fn example_renames() {
    #[derive(Debug, Serialize, Deserialize)]
    struct Example {
        pub value: i64,
    }

    let actual = SerdeArrowSchema::from_type::<Example>(
        TracingOptions::default()
            .overwrite(
                "$.value",
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
