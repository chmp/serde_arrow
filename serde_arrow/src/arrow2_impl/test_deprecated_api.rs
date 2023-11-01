#![allow(deprecated)]

use crate::{self as serde_arrow, _impl::arrow2};

#[test]
fn api_docs_serialize_into_fields() {
    use arrow2::datatypes::{DataType, Field};
    use serde::Serialize;
    use serde_arrow::arrow2::serialize_into_fields;

    #[derive(Serialize)]
    struct Record {
        a: Option<f32>,
        b: u64,
    }

    let items = vec![
        Record { a: Some(1.0), b: 2 },
        // ...
    ];

    let fields = serialize_into_fields(&items, Default::default()).unwrap();
    let expected = vec![
        Field::new("a", DataType::Float32, true),
        Field::new("b", DataType::UInt64, false),
    ];

    assert_eq!(fields, expected);
}

#[test]
fn api_docs_serialize_into_field() {
    use arrow2::datatypes::{DataType, Field};
    use serde_arrow::arrow2::serialize_into_field;

    let items: Vec<f32> = vec![1.0, 2.0, 3.0, 4.0];

    let field = serialize_into_field(&items, "floats", Default::default()).unwrap();
    assert_eq!(field, Field::new("floats", DataType::Float32, false));
}

#[test]
fn api_docs_serialize_into_arrays() {
    use serde::Serialize;
    use serde_arrow::arrow2::{serialize_into_arrays, serialize_into_fields};

    #[derive(Serialize)]
    struct Record {
        a: Option<f32>,
        b: u64,
    }

    let items = vec![
        Record { a: Some(1.0), b: 2 },
        // ...
    ];

    let fields = serialize_into_fields(&items, Default::default()).unwrap();
    let arrays = serialize_into_arrays(&fields, &items).unwrap();

    assert_eq!(arrays.len(), 2);
}

#[test]
fn api_docs_serialize_into_array() {
    use arrow2::datatypes::{DataType, Field};
    use serde_arrow::arrow2::serialize_into_array;

    let items: Vec<f32> = vec![1.0, 2.0, 3.0, 4.0];

    let field = Field::new("floats", DataType::Float32, false);
    let array = serialize_into_array(&field, &items).unwrap();

    assert_eq!(array.len(), 4);
}

#[test]
fn api_docs_deserialize_from_arrays() {
    use serde::{Deserialize, Serialize};
    use serde_arrow::{
        arrow2::{deserialize_from_arrays, serialize_into_arrays, serialize_into_fields},
        schema::TracingOptions,
    };

    #[derive(Deserialize, Serialize)]
    struct Record {
        a: Option<f32>,
        b: u64,
    }

    // provide an example record to get the field information
    let fields =
        serialize_into_fields(&[Record { a: Some(1.0), b: 2 }], TracingOptions::default()).unwrap();

    // hidden in docs:
    let items = &[Record { a: Some(1.0), b: 2 }];
    let arrays = serialize_into_arrays(&fields, &items).unwrap();

    // deserialize the records from arrays
    let items: Vec<Record> = deserialize_from_arrays(&fields, &arrays).unwrap();
    drop(items);
}

#[test]
fn api_docs_deserialize_from_array() {
    use arrow2::datatypes::{DataType, Field};
    use serde_arrow::arrow2::{deserialize_from_array, serialize_into_array};

    let field = Field::new("floats", DataType::Float32, false);

    let array = serialize_into_array(&field, &vec![1.0_f32, 2.0, 3.0]).unwrap();
    let items: Vec<f32> = deserialize_from_array(&field, &array).unwrap();

    drop(items);
}

#[test]
fn api_docs_arrays_builder() {
    use arrow2::datatypes::{DataType, Field};
    use serde::Serialize;
    use serde_arrow::arrow2::ArraysBuilder;

    #[derive(Serialize)]
    struct Record {
        a: Option<f32>,
        b: u64,
    }
    let fields = vec![
        Field::new("a", DataType::Float32, true),
        Field::new("b", DataType::UInt64, false),
    ];
    let mut builder = ArraysBuilder::new(&fields).unwrap();

    builder.push(&Record { a: Some(1.0), b: 2 }).unwrap();
    builder.push(&Record { a: Some(3.0), b: 4 }).unwrap();
    builder.push(&Record { a: Some(5.0), b: 5 }).unwrap();

    builder
        .extend(&[
            Record { a: Some(6.0), b: 7 },
            Record { a: Some(8.0), b: 9 },
            Record {
                a: Some(10.0),
                b: 11,
            },
        ])
        .unwrap();

    let arrays = builder.build_arrays().unwrap();

    assert_eq!(arrays.len(), 2);
    assert_eq!(arrays[0].len(), 6);
}

#[test]
fn api_docs_array_builder() {
    use arrow2::datatypes::{DataType, Field};
    use serde_arrow::arrow2::ArrayBuilder;

    let field = Field::new("value", DataType::Int64, false);
    let mut builder = ArrayBuilder::new(&field).unwrap();

    builder.push(&-1_i64).unwrap();
    builder.push(&2_i64).unwrap();
    builder.push(&-3_i64).unwrap();

    builder.extend(&[4_i64, -5, 6]).unwrap();

    let array = builder.build_array().unwrap();
    assert_eq!(array.len(), 6);
}
