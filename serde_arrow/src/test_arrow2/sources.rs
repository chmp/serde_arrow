use arrow2::{
    array::{Array, BooleanArray, PrimitiveArray, StructArray},
    datatypes::{DataType, Field},
};
use serde::Deserialize;

use crate::{
    arrow2::{build_dynamic_source, RecordSource},
    event::{collect_events, deserialize_from_source, Event},
    Result,
};

#[test]
fn records_source() -> Result<()> {
    let column1 = PrimitiveArray::<i8>::from(vec![Some(0), None]);
    let column2 = BooleanArray::from(vec![Some(true), Some(false)]);

    let mut source = RecordSource::new(
        vec!["a", "b"],
        vec![
            build_dynamic_source(&column1)?,
            build_dynamic_source(&column2)?,
        ],
    );

    let events = collect_events(&mut source)?;
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

    let source = RecordSource::new(
        vec!["a", "b"],
        vec![
            build_dynamic_source(&column1)?,
            build_dynamic_source(&column2)?,
        ],
    );

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
    let mut source = build_dynamic_source(&array)?;
    let actual = collect_events(&mut source)?;

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
    let mut source = RecordSource::new(vec!["s"], vec![build_dynamic_source(&array)?]);
    let actual = collect_events(&mut source)?;

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
    let source = RecordSource::new(vec!["s"], vec![build_dynamic_source(&array)?]);

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
    let source = RecordSource::new(vec!["s"], vec![build_dynamic_source(&outer)?]);

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
    let mut source = build_dynamic_source(&array)?;
    let events = collect_events(&mut source)?;

    let expected = vec![Event::I32(0), Event::Null, Event::I32(1)];

    assert_eq!(events, expected);
    Ok(())
}
