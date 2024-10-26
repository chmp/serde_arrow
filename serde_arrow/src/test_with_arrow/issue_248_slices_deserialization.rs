use std::collections::HashMap;

use serde_json::json;

use crate::_impl::arrow::datatypes::FieldRef;
use crate::internal::testing::hash_map;
use crate::utils::Item;
use crate::{
    self as serde_arrow,
    schema::{SchemaLike, TracingOptions},
};

#[test]
fn bytes_as_list() {
    #[derive(serde::Deserialize, serde::Serialize, Debug, PartialEq)]
    struct Struct {
        string: Vec<u8>,
    }
    let data = (1..=10)
        .map(|x| Struct {
            string: x.to_string().into_bytes(),
        })
        .collect::<Vec<_>>();
    let fields = Vec::<FieldRef>::from_value(json!([
        {"name": "string", "data_type": "Binary"},
    ]))
    .unwrap();
    let batch = serde_arrow::to_record_batch(&fields, &data).unwrap();
    let batch = batch.slice(5, 5);

    let actual: Vec<Struct> = serde_arrow::from_record_batch(&batch).unwrap();
    assert_eq!(data[5..10], actual);
}

#[test]
fn strings() {
    #[derive(serde::Deserialize, serde::Serialize, Debug, PartialEq)]
    struct Struct {
        string: String,
    }
    let data = (1..=10)
        .map(|x| Struct {
            string: x.to_string(),
        })
        .collect::<Vec<_>>();
    let fields = Vec::<FieldRef>::from_type::<Struct>(TracingOptions::default()).unwrap();
    let batch = serde_arrow::to_record_batch(&fields, &data).unwrap();
    let batch = batch.slice(5, 5);

    let actual: Vec<Struct> = serde_arrow::from_record_batch(&batch).unwrap();
    assert_eq!(data[5..10], actual);
}

#[test]
fn vec_of_strings() {
    #[derive(serde::Deserialize, serde::Serialize, Debug, PartialEq)]
    struct Struct {
        string: Vec<String>,
    }
    let data = (1..=10)
        .map(|x| Struct {
            string: vec![x.to_string()],
        })
        .collect::<Vec<_>>();
    let fields = Vec::<FieldRef>::from_type::<Struct>(TracingOptions::default()).unwrap();
    let batch = serde_arrow::to_record_batch(&fields, &data).unwrap();
    let batch = batch.slice(5, 5);

    let actual: Vec<Struct> = serde_arrow::from_record_batch(&batch).unwrap();
    assert_eq!(data[5..10], actual);
}

#[test]
fn map_of_strings() {
    #[derive(serde::Deserialize, serde::Serialize, Debug, PartialEq)]
    struct Struct {
        string: HashMap<String, String>,
    }
    let data = (1..=10)
        .map(|x| Struct {
            string: hash_map!(x.to_string() => x.to_string()),
        })
        .collect::<Vec<_>>();
    let fields =
        Vec::<FieldRef>::from_type::<Struct>(TracingOptions::default().map_as_struct(false))
            .unwrap();
    let batch = serde_arrow::to_record_batch(&fields, &data).unwrap();
    let batch = batch.slice(5, 5);

    let actual: Vec<Struct> = serde_arrow::from_record_batch(&batch).unwrap();
    assert_eq!(data[5..10], actual);
}

#[test]
fn enums() {
    #[derive(serde::Deserialize, serde::Serialize, Debug, PartialEq)]
    enum Value {
        I64(i64),
        I32(i32),
        Str(String),
    }

    // Note: this works, as the initial offset for the remaining variants is 0
    let data = vec![
        Item(Value::I64(0)),
        Item(Value::I64(1)),
        Item(Value::I32(2)),
        Item(Value::Str(String::from("3"))),
    ];
    let fields = Vec::<FieldRef>::from_type::<Item<Value>>(TracingOptions::default()).unwrap();

    let batch = serde_arrow::to_record_batch(&fields, &data).unwrap();
    let batch = batch.slice(2, 2);

    let actual: Vec<Item<Value>> = serde_arrow::from_record_batch(&batch).unwrap();
    assert_eq!(data[2..4], actual);
}

#[test]
fn enums_2() {
    #[derive(serde::Deserialize, serde::Serialize, Debug, PartialEq)]
    enum Value {
        I64(i64),
        I32(i32),
        Str(String),
    }

    // note use always the same variant to force the sliced offsets to result in non-zero initial
    // offset for type 0
    let data = vec![
        Item(Value::I64(0)),
        Item(Value::I64(1)),
        Item(Value::I64(2)),
        Item(Value::I64(3)),
    ];
    let fields = Vec::<FieldRef>::from_type::<Item<Value>>(TracingOptions::default()).unwrap();

    let batch = serde_arrow::to_record_batch(&fields, &data).unwrap();
    let batch = batch.slice(2, 2);

    let actual: Vec<Item<Value>> = serde_arrow::from_record_batch(&batch).unwrap();
    assert_eq!(data[2..4], actual);
}
