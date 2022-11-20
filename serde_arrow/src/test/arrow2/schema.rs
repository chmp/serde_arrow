use arrow2::datatypes::{DataType, Field};
use serde::Serialize;

use crate::arrow2::serialize_into_fields;

#[test]
fn empty() {
    let items: Vec<i8> = Vec::new();
    assert!(serialize_into_fields(&items).is_err());
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

            let fields = serialize_into_fields(&items).unwrap();
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

    let actual = serialize_into_fields(&items).unwrap();
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

    let actual = serialize_into_fields(&items).unwrap();
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

    let actual = serialize_into_fields(&items).unwrap();
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
