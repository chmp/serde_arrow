//! Test rust objects -> events -> arrays
use arrow2::{
    array::PrimitiveArray,
    bitmap::Bitmap,
    datatypes::{DataType, Field},
};
use serde::Serialize;

use crate::{
    arrow2::{
        serialize_into_arrays,
        sinks::{base::ArrayBuilder, builders::build_dynamic_array_builder},
    },
    event::{Event, EventSink},
    Result,
};

#[test]
fn primitive_sink_i8() -> Result<()> {
    let mut sink = build_dynamic_array_builder(&Field::new("value", DataType::Int8, false))?;
    sink.accept(Event::I8(13))?;
    sink.accept(Event::Null)?;
    sink.accept(Event::I8(42))?;

    let array = sink.into_array()?;

    assert_eq!(array.len(), 3);
    assert_eq!(array.is_null(0), false);
    assert_eq!(array.is_null(1), true);
    assert_eq!(array.is_null(2), false);

    Ok(())
}

#[test]
fn boolean_sink() -> Result<()> {
    let mut sink = build_dynamic_array_builder(&Field::new("value", DataType::Boolean, false))?;
    sink.accept(Event::Bool(true))?;
    sink.accept(Event::Null)?;
    sink.accept(Event::Bool(false))?;

    let array = sink.into_array()?;

    assert_eq!(array.len(), 3);
    assert_eq!(array.is_null(0), false);
    assert_eq!(array.is_null(1), true);
    assert_eq!(array.is_null(2), false);

    Ok(())
}

#[test]
fn struct_sink() -> Result<()> {
    let fields = vec![
        Field::new("a", DataType::Int8, true),
        Field::new("b", DataType::Boolean, true),
    ];

    let mut sink = build_dynamic_array_builder(&Field::new("s", DataType::Struct(fields), false))?;

    sink.accept(Event::StartMap)?;
    sink.accept(Event::Key("a"))?;
    sink.accept(Event::I8(0))?;
    sink.accept(Event::Key("b"))?;
    sink.accept(Event::Bool(false))?;
    sink.accept(Event::EndMap)?;
    sink.accept(Event::StartMap)?;
    sink.accept(Event::Key("b"))?;
    sink.accept(Event::Bool(true))?;
    sink.accept(Event::Key("a"))?;
    sink.accept(Event::Null)?;
    sink.accept(Event::EndMap)?;

    let array = sink.into_array()?;

    assert_eq!(array.len(), 2);

    Ok(())
}

#[test]
fn nested_struct_sink() -> Result<()> {
    let inner_fields = vec![Field::new("value", DataType::Boolean, false)];
    let outer_fields = vec![
        Field::new("a", DataType::Int8, true),
        Field::new("b", DataType::Struct(inner_fields), true),
    ];

    let mut sink =
        build_dynamic_array_builder(&Field::new("value", DataType::Struct(outer_fields), false))
            .unwrap();

    sink.accept(Event::StartMap).unwrap();
    sink.accept(Event::Key("a")).unwrap();
    sink.accept(Event::I8(0)).unwrap();
    sink.accept(Event::Key("b")).unwrap();
    sink.accept(Event::StartMap).unwrap();
    sink.accept(Event::Key("value")).unwrap();
    sink.accept(Event::Bool(false)).unwrap();
    sink.accept(Event::EndMap).unwrap();
    sink.accept(Event::EndMap).unwrap();
    sink.accept(Event::StartMap).unwrap();
    sink.accept(Event::Key("b")).unwrap();
    sink.accept(Event::StartMap).unwrap();
    sink.accept(Event::Key("value")).unwrap();
    sink.accept(Event::Bool(true)).unwrap();
    sink.accept(Event::EndMap).unwrap();
    sink.accept(Event::Key("a")).unwrap();
    sink.accept(Event::Null).unwrap();
    sink.accept(Event::EndMap).unwrap();

    let array = sink.into_array().unwrap();

    assert_eq!(array.len(), 2);

    Ok(())
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

    let actual0 = values[0]
        .as_any()
        .downcast_ref::<PrimitiveArray<i8>>()
        .unwrap();
    let expected0 = PrimitiveArray::<i8>::from_slice([0, 2, 4]);

    assert_eq!(actual0, &expected0);

    let actual1 = values[1]
        .as_any()
        .downcast_ref::<PrimitiveArray<i32>>()
        .unwrap();
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

    let actual0 = values[0]
        .as_any()
        .downcast_ref::<PrimitiveArray<i8>>()
        .unwrap();

    let expected0 = PrimitiveArray::<i8>::from_slice([0, 2, 4])
        .with_validity(Some(Bitmap::from([true, false, true])));

    assert_eq!(actual0, &expected0);
}
