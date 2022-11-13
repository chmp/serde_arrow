//! Test rust objects -> events -> arrays
use arrow2::datatypes::{DataType, Field};
use serde::Serialize;

use crate::{
    arrow2::sinks::{
        base::ArrayBuilder, builders::build_dynamic_array_builder, records_builder::RecordsBuilder,
    },
    event::{serialize_into_sink, Event, EventSink},
    Result,
};

#[test]
fn primitive_sink_i8() -> Result<()> {
    let mut sink = build_dynamic_array_builder(&DataType::Int8)?;
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
    let mut sink = build_dynamic_array_builder(&DataType::Boolean)?;
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

    let mut sink = build_dynamic_array_builder(&DataType::Struct(fields))?;

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

    let mut sink = build_dynamic_array_builder(&DataType::Struct(outer_fields)).unwrap();

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

fn nested_struct_serialize() -> Result<()> {
    let inner_fields = vec![Field::new("value", DataType::Boolean, false)];
    let sink = RecordsBuilder::new(vec![
        Field::new("a", DataType::Int8, true),
        Field::new("b", DataType::Struct(inner_fields), true),
    ])?;

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

    let (_fields, values) = serialize_into_sink(sink, &items)?.into_records()?;

    assert_eq!(values.len(), 2);
    assert_eq!(values[0].len(), 3);
    assert_eq!(values[1].len(), 3);

    Ok(())
}
