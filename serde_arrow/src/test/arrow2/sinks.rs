//! Test rust objects -> events -> arrays
use std::{any::Any, collections::HashMap};

use arrow2::{
    array::{Array, ListArray, PrimitiveArray, StructArray},
    bitmap::Bitmap,
    datatypes::{DataType, Field},
};
use serde::Serialize;

use crate::{
    arrow2::{serialize_into_arrays, sinks::build_array_builder},
    internal::{
        event::Event,
        sink::{ArrayBuilder, EventSink},
    },
};

fn downcast<T: Any>(array: &Box<dyn Array>) -> &T {
    array.as_any().downcast_ref::<T>().unwrap()
}

macro_rules! test_option_support {
    (test_name = $test_name:ident, data_type = $data_type:expr, value = $value:expr, ) => {
        mod $test_name {
            use arrow2::datatypes::{DataType, Field};

            use crate::{
                arrow2::sinks::build_array_builder,
                internal::{
                    event::Event,
                    sink::{ArrayBuilder, EventSink},
                },
            };

            #[test]
            fn none_first() {
                // for Null arrays Null's are considered values
                let is_non_null_data_type = !matches!($data_type, DataType::Null);
                let mut sink =
                    build_array_builder(&Field::new("value", $data_type, false)).unwrap();
                sink.accept(Event::Null).unwrap();
                sink.accept(Event::Some).unwrap();
                sink.accept($value).unwrap();
                sink.finish().unwrap();

                let array = sink.build_array().unwrap();

                assert_eq!(array.len(), 2);
                assert_eq!(array.is_null(0), is_non_null_data_type);
                assert_eq!(array.is_null(1), false);
            }

            #[test]
            fn some_first() {
                // for Null arrays Null's are considered values
                let is_non_null_data_type = !matches!($data_type, DataType::Null);
                let mut sink =
                    build_array_builder(&Field::new("value", $data_type, false)).unwrap();
                sink.accept(Event::Some).unwrap();
                sink.accept($value).unwrap();
                sink.accept(Event::Null).unwrap();
                sink.finish().unwrap();

                let array = sink.build_array().unwrap();

                assert_eq!(array.len(), 2);
                assert_eq!(array.is_null(0), false);
                assert_eq!(array.is_null(1), is_non_null_data_type);
            }

            #[test]
            fn unmarked_some() {
                // for Null arrays Null's are considered values
                let is_non_null_data_type = !matches!($data_type, DataType::Null);
                let mut sink =
                    build_array_builder(&Field::new("value", $data_type, false)).unwrap();
                sink.accept($value).unwrap();
                sink.accept(Event::Null).unwrap();
                sink.finish().unwrap();

                let array = sink.build_array().unwrap();

                assert_eq!(array.len(), 2);
                assert_eq!(array.is_null(0), false);
                assert_eq!(array.is_null(1), is_non_null_data_type);
            }
        }
    };
}

test_option_support!(
    test_name = option_support_null,
    data_type = DataType::Null,
    value = Event::Null,
);

test_option_support!(
    test_name = option_support_bool,
    data_type = DataType::Boolean,
    value = Event::Bool(true),
);

test_option_support!(
    test_name = option_support_i8,
    data_type = DataType::Int8,
    value = Event::I8(0),
);

test_option_support!(
    test_name = option_support_i16,
    data_type = DataType::Int16,
    value = Event::I16(0),
);

test_option_support!(
    test_name = option_support_f64,
    data_type = DataType::Float64,
    value = Event::F64(0.0),
);

test_option_support!(
    test_name = option_support_utf8,
    data_type = DataType::Utf8,
    value = Event::Str("foo"),
);

test_option_support!(
    test_name = option_support_large_utf8,
    data_type = DataType::LargeUtf8,
    value = Event::Str("foo"),
);

#[test]
fn primitive_sink_i8() {
    let mut sink = build_array_builder(&Field::new("value", DataType::Int8, false)).unwrap();
    sink.accept(Event::I8(13)).unwrap();
    sink.accept(Event::Null).unwrap();
    sink.accept(Event::I8(42)).unwrap();
    sink.finish().unwrap();

    let array = sink.build_array().unwrap();

    assert_eq!(array.len(), 3);
    assert_eq!(array.is_null(0), false);
    assert_eq!(array.is_null(1), true);
    assert_eq!(array.is_null(2), false);
}

#[test]
fn primitive_sink_i8_some() {
    let mut sink = build_array_builder(&Field::new("value", DataType::Int8, false)).unwrap();
    sink.accept(Event::Some).unwrap();
    sink.accept(Event::I8(13)).unwrap();
    sink.accept(Event::Null).unwrap();
    sink.accept(Event::Some).unwrap();
    sink.accept(Event::I8(42)).unwrap();
    sink.finish().unwrap();

    let array = sink.build_array().unwrap();

    assert_eq!(array.len(), 3);
    assert_eq!(array.is_null(0), false);
    assert_eq!(array.is_null(1), true);
    assert_eq!(array.is_null(2), false);
}

#[test]
fn boolean_sink() {
    let mut sink = build_array_builder(&Field::new("value", DataType::Boolean, false)).unwrap();
    sink.accept(Event::Bool(true)).unwrap();
    sink.accept(Event::Null).unwrap();
    sink.accept(Event::Bool(false)).unwrap();
    sink.finish().unwrap();

    let array = sink.build_array().unwrap();

    assert_eq!(array.len(), 3);
    assert_eq!(array.is_null(0), false);
    assert_eq!(array.is_null(1), true);
    assert_eq!(array.is_null(2), false);
}

#[test]
fn struct_sink() {
    let fields = vec![
        Field::new("a", DataType::Int8, true),
        Field::new("b", DataType::Boolean, true),
    ];

    let mut sink = build_array_builder(&Field::new("s", DataType::Struct(fields), false)).unwrap();

    sink.accept(Event::StartStruct).unwrap();
    sink.accept(Event::Str("a")).unwrap();
    sink.accept(Event::I8(0)).unwrap();
    sink.accept(Event::Str("b")).unwrap();
    sink.accept(Event::Bool(false)).unwrap();
    sink.accept(Event::EndStruct).unwrap();
    sink.accept(Event::StartStruct).unwrap();
    sink.accept(Event::Str("b")).unwrap();
    sink.accept(Event::Bool(true)).unwrap();
    sink.accept(Event::Str("a")).unwrap();
    sink.accept(Event::Null).unwrap();
    sink.accept(Event::EndStruct).unwrap();
    sink.finish().unwrap();

    let array = sink.build_array().unwrap();

    assert_eq!(array.len(), 2);
}

#[test]
fn nested_struct_sink() {
    let inner_fields = vec![Field::new("value", DataType::Boolean, false)];
    let outer_fields = vec![
        Field::new("a", DataType::Int8, true),
        Field::new("b", DataType::Struct(inner_fields), true),
    ];

    let mut sink =
        build_array_builder(&Field::new("value", DataType::Struct(outer_fields), false)).unwrap();

    sink.accept(Event::StartStruct).unwrap();
    sink.accept(Event::Str("a")).unwrap();
    sink.accept(Event::I8(0)).unwrap();
    sink.accept(Event::Str("b")).unwrap();
    sink.accept(Event::StartStruct).unwrap();
    sink.accept(Event::Str("value")).unwrap();
    sink.accept(Event::Bool(false)).unwrap();
    sink.accept(Event::EndStruct).unwrap();
    sink.accept(Event::EndStruct).unwrap();
    sink.accept(Event::StartStruct).unwrap();
    sink.accept(Event::Str("b")).unwrap();
    sink.accept(Event::StartStruct).unwrap();
    sink.accept(Event::Str("value")).unwrap();
    sink.accept(Event::Bool(true)).unwrap();
    sink.accept(Event::EndStruct).unwrap();
    sink.accept(Event::Str("a")).unwrap();
    sink.accept(Event::Null).unwrap();
    sink.accept(Event::EndStruct).unwrap();
    sink.finish().unwrap();

    let array = sink.build_array().unwrap();

    assert_eq!(array.len(), 2);
}

#[test]
fn nested_struct_serialize() {
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

    let fields = [
        Field::new("a", DataType::Int8, true),
        Field::new(
            "b",
            DataType::Struct(vec![Field::new("value", DataType::Boolean, false)]),
            true,
        ),
    ];

    let values = serialize_into_arrays(&fields, &items).unwrap();

    assert_eq!(values.len(), 2);
    assert_eq!(values[0].len(), 3);
    assert_eq!(values[1].len(), 3);
}

#[test]
fn into_arrays_simple() {
    #[derive(Debug, Serialize)]
    struct Item {
        a: i8,
        b: i32,
    }

    let items = vec![
        Item { a: 0, b: 1 },
        Item { a: 2, b: 3 },
        Item { a: 4, b: 5 },
    ];

    let fields = [
        Field::new("a", DataType::Int8, false),
        Field::new("b", DataType::Int32, false),
    ];

    let values = serialize_into_arrays(&fields, &items).unwrap();

    assert_eq!(values.len(), 2);

    let actual0 = downcast::<PrimitiveArray<i8>>(&values[0]);
    let expected0 = PrimitiveArray::<i8>::from_slice([0, 2, 4]);

    assert_eq!(actual0, &expected0);

    let actual1 = downcast::<PrimitiveArray<i32>>(&values[1]);
    let expected1 = PrimitiveArray::<i32>::from_slice([1, 3, 5]);

    assert_eq!(actual1, &expected1);
}

#[test]
fn into_arrays_option_i8() {
    #[derive(Debug, Serialize)]
    struct Item {
        a: Option<i8>,
    }

    let items = vec![Item { a: Some(0) }, Item { a: None }, Item { a: Some(4) }];
    let fields = [Field::new("a", DataType::Int8, true)];

    let values = serialize_into_arrays(&fields, &items).unwrap();

    assert_eq!(values.len(), 1);

    let actual0 = downcast::<PrimitiveArray<i8>>(&values[0]);
    let expected0 = PrimitiveArray::<i8>::from_slice([0, 2, 4])
        .with_validity(Some(Bitmap::from([true, false, true])));

    assert_eq!(actual0, &expected0);
}

#[test]
fn nested_list_serialize() {
    #[derive(Debug, Serialize)]
    struct Item {
        a: Vec<i8>,
    }

    let items = vec![
        Item { a: vec![0, 1, 2] },
        Item { a: vec![3, 4] },
        Item { a: vec![5] },
    ];

    let fields = [Field::new(
        "a",
        DataType::LargeList(Box::new(Field::new("a", DataType::Int8, false))),
        true,
    )];

    let values = serialize_into_arrays(&fields, &items).unwrap();

    assert_eq!(values.len(), 1);
    assert_eq!(values[0].len(), 3);

    let column0 = downcast::<ListArray<i64>>(&values[0]);
    assert_eq!(column0.is_null(0), false);
    assert_eq!(
        downcast::<PrimitiveArray<i8>>(&column0.value(0)),
        &PrimitiveArray::<i8>::from_slice([0, 1, 2]),
    );
    assert_eq!(column0.is_null(1), false);
    assert_eq!(
        downcast::<PrimitiveArray<i8>>(&column0.value(1)),
        &PrimitiveArray::<i8>::from_slice([3, 4]),
    );
    assert_eq!(column0.is_null(2), false);
    assert_eq!(
        downcast::<PrimitiveArray<i8>>(&column0.value(2)),
        &PrimitiveArray::<i8>::from_slice([5]),
    );
}

#[test]
fn nested_list_nulls_serialize() {
    #[derive(Debug, Serialize)]
    struct Item {
        a: Option<Vec<i8>>,
    }

    let items = vec![
        Item { a: Some(vec![]) },
        Item { a: None },
        Item { a: Some(vec![5]) },
    ];

    let fields = [Field::new(
        "a",
        DataType::LargeList(Box::new(Field::new("a", DataType::Int8, false))),
        true,
    )];

    let values = serialize_into_arrays(&fields, &items).unwrap();

    assert_eq!(values.len(), 1);
    assert_eq!(values[0].len(), 3);

    let column0 = values[0].as_any().downcast_ref::<ListArray<i64>>().unwrap();
    assert_eq!(column0.is_null(0), false);
    assert_eq!(
        downcast::<PrimitiveArray<i8>>(&column0.value(0)),
        &PrimitiveArray::<i8>::from_slice([]),
    );
    assert_eq!(column0.is_null(1), true);
    assert_eq!(
        downcast::<PrimitiveArray<i8>>(&column0.value(1)),
        &PrimitiveArray::<i8>::from_slice([]),
    );
    assert_eq!(column0.is_null(2), false);
    assert_eq!(
        downcast::<PrimitiveArray<i8>>(&column0.value(2)),
        &PrimitiveArray::<i8>::from_slice([5]),
    );
}

#[test]
fn nested_list_structs_serialize() {
    #[derive(Debug, Serialize)]
    struct Item {
        a: Vec<Inner>,
    }

    #[derive(Debug, Serialize)]
    struct Inner {
        a: i8,
        b: i32,
    }

    let items = vec![
        Item {
            a: vec![Inner { a: 0, b: 1 }, Inner { a: 2, b: 3 }],
        },
        Item { a: vec![] },
        Item {
            a: vec![Inner { a: 4, b: 5 }],
        },
    ];

    let inner = DataType::Struct(vec![
        Field::new("a", DataType::Int8, false),
        Field::new("b", DataType::Int32, false),
    ]);
    let fields = [Field::new(
        "a",
        DataType::LargeList(Box::new(Field::new("a", inner, false))),
        true,
    )];

    let values = serialize_into_arrays(&fields, &items).unwrap();

    let actual0 = downcast::<ListArray<i64>>(&values[0]);
    let item0 = actual0.value(0);
    let item0 = downcast::<StructArray>(&item0);
    assert_eq!(item0.fields()[0].name, "a");
    assert_eq!(item0.fields()[1].name, "b");

    let children0 = item0.values();
    assert_eq!(
        downcast::<PrimitiveArray<i8>>(&children0[0]),
        &PrimitiveArray::<i8>::from_slice([0, 2])
    );
    assert_eq!(
        downcast::<PrimitiveArray<i32>>(&children0[1]),
        &PrimitiveArray::<i32>::from_slice([1, 3])
    );
}

#[test]
fn into_outer_maps_simple() {
    let mut items = Vec::new();

    let mut element = HashMap::<String, i32>::new();
    element.insert(String::from("a"), 0);
    element.insert(String::from("b"), 1);
    items.push(element);

    let mut element = HashMap::<String, i32>::new();
    element.insert(String::from("a"), 2);
    element.insert(String::from("b"), 3);
    items.push(element);

    let fields = [
        Field::new("a", DataType::Int32, false),
        Field::new("b", DataType::Int32, false),
    ];

    let values = serialize_into_arrays(&fields, &items).unwrap();

    assert_eq!(values.len(), 2);

    let actual0 = downcast::<PrimitiveArray<i32>>(&values[0]);
    let expected0 = PrimitiveArray::<i32>::from_slice([0, 2]);

    assert_eq!(actual0, &expected0);

    let actual1 = downcast::<PrimitiveArray<i32>>(&values[1]);
    let expected1 = PrimitiveArray::<i32>::from_slice([1, 3]);

    assert_eq!(actual1, &expected1);
}
