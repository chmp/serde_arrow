use arrow2::{
    array::{Array, BooleanArray, PrimitiveArray, StructArray},
    datatypes::{DataType, Field},
};
use serde::Deserialize;

use crate::{
    arrow2::sources::{build_dynamic_source, build_record_source, ListSource},
    base::{
        collect_events, deserialize_from_source, source::IntoEventSource, DynamicSource, Event,
        EventSource,
    },
    Result,
};

#[test]
fn records_source() -> Result<()> {
    let column1 = PrimitiveArray::<i8>::from(vec![Some(0), None]);
    let column2 = BooleanArray::from(vec![Some(true), Some(false)]);

    let fields = [
        Field::new("a", column1.data_type().clone(), false),
        Field::new("b", column2.data_type().clone(), false),
    ];
    let arrays = [&column1 as &dyn Array, &column2 as &dyn Array];
    let source = build_record_source(&fields, &arrays)?;

    let events = collect_events(source)?;
    let expected = vec![
        Event::StartSequence,
        Event::StartMap,
        Event::owned_key("a"),
        Event::I8(0),
        Event::owned_key("b"),
        Event::Bool(true),
        Event::EndMap,
        Event::StartMap,
        Event::owned_key("a"),
        Event::Null,
        Event::owned_key("b"),
        Event::Bool(false),
        Event::EndMap,
        Event::EndSequence,
    ];

    assert_eq!(events, expected);

    Ok(())
}

#[test]
fn deserialize_from_record_source() -> Result<()> {
    #[derive(Debug, Clone, PartialEq, Eq, Deserialize)]
    struct Item {
        a: Option<i8>,
        b: bool,
    }

    let column1 = PrimitiveArray::<i8>::from(vec![Some(0), None]);
    let column2 = BooleanArray::from(vec![Some(true), Some(false)]);

    let fields = [
        Field::new("a", column1.data_type().clone(), false),
        Field::new("b", column2.data_type().clone(), false),
    ];
    let arrays = [&column1 as &dyn Array, &column2 as &dyn Array];
    let source = build_record_source(&fields, &arrays)?;

    let actual: Vec<Item> = deserialize_from_source(source)?;

    let expected = vec![
        Item {
            a: Some(0),
            b: true,
        },
        Item { a: None, b: false },
    ];

    assert_eq!(actual, expected);

    Ok(())
}

#[test]
fn struct_source_events() -> Result<()> {
    #[derive(Debug, Clone, PartialEq, Eq, Deserialize)]
    struct Item {
        a: Option<i8>,
        b: bool,
    }

    let array = StructArray::new(
        DataType::Struct(vec![
            Field::new("a", DataType::Int8, true),
            Field::new("b", DataType::Boolean, false),
        ]),
        vec![
            Box::new(PrimitiveArray::<i8>::from(vec![Some(0), None])) as _,
            Box::new(BooleanArray::from(vec![Some(true), Some(false)])) as _,
        ],
        None,
    );
    let field = Field::new("s", array.data_type().clone(), false);
    let source = build_dynamic_source(&field, &array)?;
    let actual = collect_events(source)?;

    let expected = vec![
        Event::StartMap,
        Event::owned_key("a"),
        Event::I8(0),
        Event::owned_key("b"),
        Event::Bool(true),
        Event::EndMap,
        Event::StartMap,
        Event::owned_key("a"),
        Event::Null,
        Event::owned_key("b"),
        Event::Bool(false),
        Event::EndMap,
    ];

    assert_eq!(actual, expected);

    Ok(())
}

#[test]
fn deserialize_struct_events() -> Result<()> {
    let array = StructArray::new(
        DataType::Struct(vec![
            Field::new("a", DataType::Int8, true),
            Field::new("b", DataType::Boolean, false),
        ]),
        vec![
            Box::new(PrimitiveArray::<i8>::from(vec![Some(0), None])) as _,
            Box::new(BooleanArray::from(vec![Some(true), Some(false)])) as _,
        ],
        None,
    );
    let fields = [Field::new("s", array.data_type().clone(), false)];
    let arrays = [&array as &dyn Array];
    let source = build_record_source(&fields, &arrays)?;

    let actual = collect_events(source)?;

    let expected = vec![
        Event::StartSequence,
        Event::StartMap,
        Event::owned_key("s"),
        Event::StartMap,
        Event::owned_key("a"),
        Event::I8(0),
        Event::owned_key("b"),
        Event::Bool(true),
        Event::EndMap,
        Event::EndMap,
        Event::StartMap,
        Event::owned_key("s"),
        Event::StartMap,
        Event::owned_key("a"),
        Event::Null,
        Event::owned_key("b"),
        Event::Bool(false),
        Event::EndMap,
        Event::EndMap,
        Event::EndSequence,
    ];

    assert_eq!(actual, expected);

    Ok(())
}

#[test]
fn deserialize_structs() -> Result<()> {
    let array = StructArray::new(
        DataType::Struct(vec![
            Field::new("a", DataType::Int8, true),
            Field::new("b", DataType::Boolean, false),
        ]),
        vec![
            Box::new(PrimitiveArray::<i8>::from(vec![Some(0), None])) as _,
            Box::new(BooleanArray::from(vec![Some(true), Some(false)])) as _,
        ],
        None,
    );

    let fields = [Field::new("s", array.data_type().clone(), false)];
    let arrays = [&array as &dyn Array];
    let source = build_record_source(&fields, &arrays)?;

    #[derive(Debug, Clone, PartialEq, Eq, Deserialize)]
    struct Struct {
        a: Option<i8>,
        b: bool,
    }

    #[derive(Debug, Clone, PartialEq, Eq, Deserialize)]
    struct Item {
        s: Struct,
    }

    let actual: Vec<Item> = deserialize_from_source(source)?;
    let expected = vec![
        Item {
            s: Struct {
                a: Some(0),
                b: true,
            },
        },
        Item {
            s: Struct { a: None, b: false },
        },
    ];

    assert_eq!(actual, expected);

    Ok(())
}

#[test]
fn deserialize_nested_structs() -> Result<()> {
    let inner = StructArray::new(
        DataType::Struct(vec![Field::new("value", DataType::Int8, true)]),
        vec![Box::new(PrimitiveArray::<i8>::from(vec![Some(0), None])) as _],
        None,
    );

    let outer = StructArray::new(
        DataType::Struct(vec![
            Field::new("a", inner.data_type().clone(), false),
            Field::new("b", DataType::Boolean, false),
        ]),
        vec![
            Box::new(inner) as _,
            Box::new(BooleanArray::from(vec![Some(true), Some(false)])) as _,
        ],
        None,
    );

    let fields = [Field::new("s", outer.data_type().clone(), false)];
    let arrays = [&outer as &dyn Array];
    let source = build_record_source(&fields, &arrays)?;

    #[derive(Debug, Clone, PartialEq, Eq, Deserialize)]
    struct Item {
        s: Outer,
    }

    #[derive(Debug, Clone, PartialEq, Eq, Deserialize)]
    struct Outer {
        a: Inner,
        b: bool,
    }

    #[derive(Debug, Clone, PartialEq, Eq, Deserialize)]
    struct Inner {
        value: Option<i8>,
    }

    let actual: Vec<Item> = deserialize_from_source(source)?;
    let expected = vec![
        Item {
            s: Outer {
                a: Inner { value: Some(0) },
                b: true,
            },
        },
        Item {
            s: Outer {
                a: Inner { value: None },
                b: false,
            },
        },
    ];

    assert_eq!(actual, expected);

    Ok(())
}

#[test]
fn primitive_source() -> Result<()> {
    let array = PrimitiveArray::from(vec![Some(0), None, Some(1)]);
    let field = Field::new("a", array.data_type().clone(), true);
    let source = build_dynamic_source(&field, &array)?;
    let events = collect_events(source)?;

    let expected = vec![Event::I32(0), Event::Null, Event::I32(1)];

    assert_eq!(events, expected);
    Ok(())
}

#[test]
fn list_source_no_content_nulls() {
    let events: Vec<Event<'static>> = vec![];
    let mut source = ListSource::new(
        DynamicSource::new(events.into_event_source()),
        vec![0, 0, 0, 0],
        vec![false, false, false],
    );

    assert_eq!(source.next().unwrap(), Some(Event::Null));
    assert_eq!(source.next().unwrap(), Some(Event::Null));
    assert_eq!(source.next().unwrap(), Some(Event::Null));
    assert_eq!(source.next().unwrap(), None);
}

#[test]
fn list_source_no_content_empty() {
    let events: Vec<Event<'static>> = vec![];
    let source = ListSource::new(
        DynamicSource::new(events.into_event_source()),
        vec![0, 0, 0, 0],
        vec![true, true, true],
    );
    let actual = collect_events(source).unwrap();
    let expected = vec![
        Event::StartSequence,
        Event::EndSequence,
        Event::StartSequence,
        Event::EndSequence,
        Event::StartSequence,
        Event::EndSequence,
    ];
    assert_eq!(actual, expected);
}

#[test]
fn list_source_no_content_single_items() {
    let events: Vec<Event<'static>> = vec![Event::I8(13), Event::I8(21), Event::I8(42)];
    let source = ListSource::new(
        DynamicSource::new(events.into_event_source()),
        vec![0, 1, 2, 3],
        vec![true, true, true],
    );

    let actual = collect_events(source).unwrap();
    let expected = vec![
        Event::StartSequence,
        Event::I8(13),
        Event::EndSequence,
        Event::StartSequence,
        Event::I8(21),
        Event::EndSequence,
        Event::StartSequence,
        Event::I8(42),
        Event::EndSequence,
    ];

    assert_eq!(actual, expected);
}

#[test]
fn list_source_no_content_multiple_items() {
    let events: Vec<Event<'static>> = vec![
        Event::I8(0),
        Event::I8(1),
        Event::I8(2),
        Event::I8(3),
        Event::I8(4),
        Event::I8(5),
    ];
    let source = ListSource::new(
        DynamicSource::new(events.into_event_source()),
        vec![0, 3, 5, 6],
        vec![true, true, true],
    );

    let actual = collect_events(source).unwrap();
    let expected = vec![
        Event::StartSequence,
        Event::I8(0),
        Event::I8(1),
        Event::I8(2),
        Event::EndSequence,
        Event::StartSequence,
        Event::I8(3),
        Event::I8(4),
        Event::EndSequence,
        Event::StartSequence,
        Event::I8(5),
        Event::EndSequence,
    ];
    assert_eq!(actual, expected);
}

#[test]
fn list_source_nested() {
    let events: Vec<Event<'static>> = vec![
        Event::StartSequence,
        Event::I8(0),
        Event::I8(1),
        Event::EndSequence,
        Event::StartSequence,
        Event::I8(2),
        Event::I8(3),
        Event::EndSequence,
        Event::StartSequence,
        Event::I8(4),
        Event::I8(5),
        Event::EndSequence,
    ];
    let source = ListSource::new(
        DynamicSource::new(events.into_event_source()),
        vec![0, 2, 3],
        vec![true, true],
    );

    let actual = collect_events(source).unwrap();
    let expected = vec![
        Event::StartSequence,
        Event::StartSequence,
        Event::I8(0),
        Event::I8(1),
        Event::EndSequence,
        Event::StartSequence,
        Event::I8(2),
        Event::I8(3),
        Event::EndSequence,
        Event::EndSequence,
        Event::StartSequence,
        Event::StartSequence,
        Event::I8(4),
        Event::I8(5),
        Event::EndSequence,
        Event::EndSequence,
    ];
    assert_eq!(actual, expected);
}

#[test]
fn list_source_structs() {
    let events: Vec<Event<'static>> = vec![
        Event::StartMap,
        Event::owned_key("a"),
        Event::I8(0),
        Event::owned_key("b"),
        Event::I8(1),
        Event::EndMap,
        Event::StartMap,
        Event::owned_key("a"),
        Event::I8(2),
        Event::owned_key("b"),
        Event::I8(3),
        Event::EndMap,
        Event::StartMap,
        Event::owned_key("a"),
        Event::I8(4),
        Event::I8(5),
        Event::owned_key("b"),
        Event::EndMap,
    ];
    let source = ListSource::new(
        DynamicSource::new(events.into_event_source()),
        vec![0, 2, 3],
        vec![true, true],
    );

    let actual = collect_events(source).unwrap();
    let expected = vec![
        Event::StartSequence,
        Event::StartMap,
        Event::owned_key("a"),
        Event::I8(0),
        Event::owned_key("b"),
        Event::I8(1),
        Event::EndMap,
        Event::StartMap,
        Event::owned_key("a"),
        Event::I8(2),
        Event::owned_key("b"),
        Event::I8(3),
        Event::EndMap,
        Event::EndSequence,
        Event::StartSequence,
        Event::StartMap,
        Event::owned_key("a"),
        Event::I8(4),
        Event::I8(5),
        Event::owned_key("b"),
        Event::EndMap,
        Event::EndSequence,
    ];
    assert_eq!(actual, expected);
}
