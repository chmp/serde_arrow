//! Test the schema tracing on the serde level
use std::collections::HashMap;

use crate::{
    _impl::arrow2::datatypes::{DataType, Field, UnionMode},
    internal::schema::TracingOptions,
};
use serde::Serialize;
use serde_json::json;

use crate::{arrow2::serialize_into_fields, schema::Strategy};

#[test]
fn empty() {
    let items: Vec<i8> = Vec::new();
    assert!(serialize_into_fields(&items, Default::default()).is_err());
}

macro_rules! define_primitive_test {
    ($test:ident, $ty:ty, $variant:ident) => {
        #[test]
        fn $test() {
            #[derive(Serialize)]
            struct Item {
                a: $ty,
            }
            let items: Vec<Item> = vec![Item {
                a: Default::default(),
            }];

            let fields = serialize_into_fields(&items, Default::default()).unwrap();
            let expected = vec![Field::new("a", DataType::$variant, false)];

            assert_eq!(fields, expected);
        }
    };
}

define_primitive_test!(single_bool, bool, Boolean);
define_primitive_test!(single_int8, i8, Int8);
define_primitive_test!(single_int16, i16, Int16);
define_primitive_test!(single_int32, i32, Int32);
define_primitive_test!(single_int64, i64, Int64);
define_primitive_test!(single_uint8, u8, UInt8);
define_primitive_test!(single_uint16, u16, UInt16);
define_primitive_test!(single_uint32, u32, UInt32);
define_primitive_test!(single_uint64, u64, UInt64);
define_primitive_test!(single_float32, f32, Float32);
define_primitive_test!(single_float64, f64, Float64);
define_primitive_test!(single_string, String, LargeUtf8);

#[test]
fn option_only_nulls() {
    #[derive(Debug, Serialize)]
    struct Item {
        a: Option<i8>,
    }

    let items = vec![Item { a: None }, Item { a: None }, Item { a: None }];

    // per default none only fields are not allowed
    assert!(serialize_into_fields(&items, TracingOptions::default()).is_err());

    let actual =
        serialize_into_fields(&items, TracingOptions::default().allow_null_fields(true)).unwrap();
    let expected = vec![Field::new("a", DataType::Null, true)];

    assert_eq!(actual, expected);
}

#[test]
fn option_only_somes() {
    #[derive(Debug, Serialize)]
    struct Item {
        a: Option<i8>,
    }

    let items = vec![Item { a: Some(0) }];

    let actual = serialize_into_fields(&items, Default::default()).unwrap();
    let expected = vec![Field::new("a", DataType::Int8, true)];

    assert_eq!(actual, expected);
}

#[test]
fn nested_struct() {
    #[derive(Debug, Serialize)]
    struct Item {
        b: Inner,
    }

    #[derive(Debug, Serialize)]
    struct Inner {
        value: bool,
    }

    let items = vec![
        Item {
            b: Inner { value: true },
        },
        Item {
            b: Inner { value: false },
        },
    ];

    let actual = serialize_into_fields(&items, Default::default()).unwrap();
    let expected = vec![Field::new(
        "b",
        DataType::Struct(vec![Field::new("value", DataType::Boolean, false)]),
        false,
    )];

    assert_eq!(actual, expected);
}

#[test]
fn complex_example() {
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

    let actual = serialize_into_fields(&items, Default::default()).unwrap();
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
fn union_example() {
    #[derive(Debug, Serialize)]
    struct Item {
        item: Inner,
    }

    #[derive(Debug, Serialize)]
    enum Inner {
        U8(u8),
        I32(i32),
    }

    let items = vec![
        Item {
            item: Inner::U8(21),
        },
        Item {
            item: Inner::I32(42),
        },
    ];

    let actual = serialize_into_fields(&items, Default::default()).unwrap();
    let expected = vec![Field::new(
        "item",
        DataType::Union(
            vec![
                Field::new("U8", DataType::UInt8, false),
                Field::new("I32", DataType::Int32, false),
            ],
            None,
            UnionMode::Dense,
        ),
        false,
    )];

    assert_eq!(actual, expected);
}

/// Test that using an outer map as the structuring element is supported.
///
/// Use `#[serde(flatten)]` to trigger this case.
///
#[test]
fn outer_map() {
    #[derive(Debug, Serialize)]
    struct Item {
        a: i8,
        #[serde(flatten)]
        b: Inner,
    }

    #[derive(Debug, Serialize)]
    struct Inner {
        value: bool,
    }

    let items = vec![Item {
        a: 0,
        b: Inner { value: true },
    }];

    let actual = serialize_into_fields(&items, Default::default()).unwrap();
    let expected = vec![
        Field::new("a", DataType::Int8, false),
        Field::new("value", DataType::Boolean, false),
    ];

    assert_eq!(actual, expected);
}

/// Test that using an outer map as the structuring element and that fields not
/// encountered in every record are marked as nullable
///
#[test]
fn outer_map_missing_fields() {
    let mut items = vec![];

    let mut element = HashMap::<String, i32>::new();
    element.insert(String::from("a"), 0);
    element.insert(String::from("c"), 1);
    items.push(element);

    let mut element = HashMap::<String, i32>::new();
    element.insert(String::from("b"), 2);
    element.insert(String::from("c"), 3);
    items.push(element);

    let mut actual = serialize_into_fields(&items, Default::default()).unwrap();
    actual.sort_by(|a, b| a.name.cmp(&b.name));

    let expected = vec![
        Field::new("a", DataType::Int32, true),
        Field::new("b", DataType::Int32, true),
        Field::new("c", DataType::Int32, false),
    ];

    assert_eq!(actual, expected);
}

/// Test that inconsistent types are detected
#[ignore = "Detecting inconsistent types is not yet supported"]
#[test]
fn inconsistent_types() {
    let items = json!([
        {"value": 1},
        {"value": true},
    ]);

    let actual = serialize_into_fields(&items, Default::default()).unwrap();

    let expected = vec![Field::new("value", DataType::Null, false)
        .with_metadata(Strategy::InconsistentTypes.into())];

    assert_eq!(actual, expected);
}
