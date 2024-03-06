use std::sync::Arc;

use serde::Deserialize;

use crate::{
    self as serde_arrow,
    _impl::arrow::_raw::{
        array::{LargeStringArray, RecordBatch, StringArray},
        schema::{DataType, Field, Schema},
    },
    internal::error::PanicOnError,
    schema::{SchemaLike, TracingOptions},
};

#[derive(Deserialize, Debug, PartialEq, Eq)]
struct Record {
    id: String,
}

#[test]
fn test_short_strings() -> PanicOnError<()> {
    let column: StringArray = vec!["foo", "bar"].into();

    let fields = vec![Field::new("id", DataType::Utf8, false)];

    let batch = RecordBatch::try_new(
        Arc::new(Schema::new(fields.clone())),
        vec![Arc::new(column.clone())],
    )?;

    let records: Vec<Record> = serde_arrow::from_arrow(&fields, batch.columns())?;

    assert_eq!(
        records,
        vec![
            Record {
                id: "foo".to_string()
            },
            Record {
                id: "bar".to_string()
            },
        ]
    );

    Ok(())
}

#[test]
fn test_large_strings_into_short_strings() -> PanicOnError<()> {
    let column: LargeStringArray = vec!["foo", "bar"].into();

    let deser_fields = vec![Field::new("id", DataType::Utf8, false)];
    let batch_fields = vec![Field::new("id", DataType::LargeUtf8, false)];

    assert_eq!(
        Vec::<Field>::from_type::<Record>(TracingOptions::default())?,
        batch_fields
    );

    let batch = RecordBatch::try_new(
        Arc::new(Schema::new(batch_fields.clone())),
        vec![Arc::new(column.clone())],
    )?;

    let result: Result<Vec<Record>, _> = serde_arrow::from_arrow(&deser_fields, batch.columns());

    assert!(
        result.is_err(),
        "Reading large strings into a short strings array did not error"
    );

    Ok(())
}

#[test]
fn test_large_strings() -> PanicOnError<()> {
    let column: LargeStringArray = vec!["foo", "bar"].into();

    let fields = vec![Field::new("id", DataType::LargeUtf8, false)];
    assert_eq!(
        Vec::<Field>::from_type::<Record>(TracingOptions::default())?,
        fields
    );

    let batch = RecordBatch::try_new(
        Arc::new(Schema::new(fields.clone())),
        vec![Arc::new(column.clone())],
    )?;

    let records: Vec<Record> = serde_arrow::from_arrow(&fields, batch.columns())?;

    assert_eq!(
        records,
        vec![
            Record {
                id: "foo".to_string()
            },
            Record {
                id: "bar".to_string()
            },
        ]
    );

    Ok(())
}

#[test]
fn test_short_strings_into_large_strings() -> PanicOnError<()> {
    let column: StringArray = vec!["foo", "bar"].into();

    let deser_fields = Vec::<_>::from_type::<Record>(TracingOptions::default())?;
    let batch_fields = vec![Field::new("id", DataType::Utf8, false)];

    let batch = RecordBatch::try_new(
        Arc::new(Schema::new(batch_fields.clone())),
        vec![Arc::new(column.clone())],
    )?;

    let records: Vec<Record> = serde_arrow::from_arrow(&deser_fields, batch.columns())?;

    assert_eq!(
        records,
        vec![
            Record {
                id: "foo".to_string()
            },
            Record {
                id: "bar".to_string()
            },
        ]
    );

    Ok(())
}
