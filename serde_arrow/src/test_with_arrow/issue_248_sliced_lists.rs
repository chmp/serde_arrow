use std::collections::HashMap;

use crate::_impl::arrow::datatypes::FieldRef;
use crate::internal::testing::hash_map;
use crate::{
    self as serde_arrow,
    schema::{SchemaLike, TracingOptions},
};

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
