//! Test arrow integration

use std::sync::Arc;

use serde::{Deserialize, Serialize};
use serde_json::json;

use crate::{
    self as serde_arrow,
    _impl::{
        arrow::{
            _raw::{array::RecordBatch, schema::Schema},
            datatypes::Field,
        },
        PanicOnError,
    },
    schema::{SchemaLike, TracingOptions},
    utils::Item,
    Result,
};

#[test]
fn example() -> PanicOnError<()> {
    #[derive(Debug, PartialEq, Serialize, Deserialize)]
    struct Record {
        a: i32,
        b: String,
    }

    let items_input = vec![
        Record {
            a: 21,
            b: String::from("first"),
        },
        Record {
            a: 42,
            b: String::from("second"),
        },
    ];

    let fields_from_type = Vec::<Field>::from_type::<Record>(TracingOptions::default())?;
    let arrays = serde_arrow::to_arrow(&fields_from_type, &items_input)?;

    let batch = RecordBatch::try_new(Arc::new(Schema::new(fields_from_type.clone())), arrays)?;

    let fields = Vec::<Field>::from_value(&batch.schema())?;
    let items: Vec<Record> = serde_arrow::from_arrow(&fields, batch.columns())?;

    assert_eq!(fields, fields_from_type);
    assert_eq!(items, items_input);

    Ok(())
}

#[test]
fn examples_trace_from_type() {
    assert_schema_eq_from_type::<Item<bool>>().unwrap();

    assert_schema_eq_from_type::<Item<u8>>().unwrap();
    assert_schema_eq_from_type::<Item<u16>>().unwrap();
    assert_schema_eq_from_type::<Item<u32>>().unwrap();
    assert_schema_eq_from_type::<Item<u64>>().unwrap();

    assert_schema_eq_from_type::<Item<i8>>().unwrap();
    assert_schema_eq_from_type::<Item<i16>>().unwrap();
    assert_schema_eq_from_type::<Item<i32>>().unwrap();
    assert_schema_eq_from_type::<Item<i64>>().unwrap();

    assert_schema_eq_from_type::<Item<f32>>().unwrap();
    assert_schema_eq_from_type::<Item<f64>>().unwrap();

    assert_schema_eq_from_type::<Item<Vec<i32>>>().unwrap();

    #[derive(Debug, PartialEq, Serialize, Deserialize)]
    struct Struct {
        a: i32,
        b: String,
    }

    assert_schema_eq_from_type::<Item<Struct>>().unwrap();
    assert_schema_eq_from_type::<Item<Vec<Struct>>>().unwrap();

    fn assert_schema_eq_from_type<'de, T: Serialize + Deserialize<'de>>() -> Result<()> {
        let fields_from_type = Vec::<Field>::from_type::<T>(TracingOptions::default())?;

        let items = Vec::<T>::new();
        let arrays = serde_arrow::to_arrow(&fields_from_type, &items)?;
        let batch = RecordBatch::try_new(Arc::new(Schema::new(fields_from_type.clone())), arrays)?;
        let fields_from_batch = Vec::<Field>::from_value(&batch.schema())?;

        assert_eq!(fields_from_batch, fields_from_type);
        Ok(())
    }
}

#[test]
fn examples_trace_from_value() {
    assert_schema_eq_from_value(&json!([{"name": "item", "data_type": "Null"}])).unwrap();
    assert_schema_eq_from_value(&json!([{"name": "item", "data_type": "F16"}])).unwrap();
    assert_schema_eq_from_value(&json!([{"name": "item", "data_type": "Utf8"}])).unwrap();
    assert_schema_eq_from_value(&json!([{"name": "item", "data_type": "LargeUtf8"}])).unwrap();
    assert_schema_eq_from_value(&json!([{"name": "item", "data_type": "Decimal128(4, 3)"}]))
        .unwrap();
    assert_schema_eq_from_value(&json!([{"name": "item", "data_type": "Date64"}])).unwrap();

    assert_schema_eq_from_value(&json!([{
        "name": "item",
        "data_type": "List",
        "children": [{"name": "item", "data_type": "U8"}],
    }]))
    .unwrap();
    assert_schema_eq_from_value(&json!([{
        "name": "item",
        "data_type": "LargeList",
        "children": [{"name": "item", "data_type": "U8"}],
    }]))
    .unwrap();

    fn assert_schema_eq_from_value<'de, T: Serialize + Deserialize<'de>>(value: &T) -> Result<()> {
        let fields_from_type = Vec::<Field>::from_value(value)?;

        #[derive(Debug, Serialize, Deserialize)]
        pub struct Record {}

        let items = Vec::<Record>::new();
        let arrays = serde_arrow::to_arrow(&fields_from_type, &items)?;
        let batch = RecordBatch::try_new(Arc::new(Schema::new(fields_from_type.clone())), arrays)?;
        let fields_from_batch = Vec::<Field>::from_value(&batch.schema())?;

        assert_eq!(fields_from_batch, fields_from_type);
        Ok(())
    }
}
