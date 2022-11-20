use arrow2::datatypes::{DataType, Field};
use serde::Serialize;

use crate::arrow2::serialize_into_fields;

#[test]
fn test_traced_schema() {
    #[derive(Debug, Serialize)]
    struct Item {
        a: Option<i8>,
        b: Inner,
    }

    #[derive(Debug, Serialize)]
    struct Inner {
        value: bool,
    }

    let items = vec![
        Item {
            a: Some(0),
            b: Inner { value: true },
        },
        Item {
            a: None,
            b: Inner { value: false },
        },
        Item {
            a: Some(21),
            b: Inner { value: false },
        },
    ];

    let actual = serialize_into_fields(&items).unwrap();
    let expected = vec![
        Field::new("a", DataType::Int8, true),
        Field::new(
            "b",
            DataType::Struct(vec![Field::new("value", DataType::Boolean, false)]),
            false,
        ),
    ];

    assert_eq!(actual, expected);
}

#[test]
fn test_traced_schema_only_nulls() {
    #[derive(Debug, Serialize)]
    struct Item {
        a: Option<i8>,
    }

    let items = vec![Item { a: None }, Item { a: None }, Item { a: None }];

    let actual = serialize_into_fields(&items).unwrap();
    let expected = vec![Field::new("a", DataType::Null, true)];

    assert_eq!(actual, expected);
}

#[test]
fn test_traced_schema_only_somes() {
    #[derive(Debug, Serialize)]
    struct Item {
        a: Option<i8>,
    }

    let items = vec![Item { a: Some(0) }];

    let actual = serialize_into_fields(&items).unwrap();
    let expected = vec![Field::new("a", DataType::Int8, true)];

    assert_eq!(actual, expected);
}
