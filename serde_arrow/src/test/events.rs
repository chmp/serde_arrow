use crate::{
    base::{deserialize_from_source, serialize_into_sink, Event},
    Result,
};
use serde::{Deserialize, Serialize};

#[test]
fn example() -> Result<()> {
    #[derive(Serialize, Deserialize, PartialEq, Eq, Debug)]
    struct Example {
        int8: i8,
        int32: i32,
    }

    let items = &[
        Example { int8: 0, int32: 21 },
        Example { int8: 1, int32: 42 },
    ];

    let mut events = Vec::new();
    serialize_into_sink(&mut events, &items)?;
    let expected = vec![
        Event::StartSequence,
        Event::StartStruct,
        Event::Str("int8"),
        Event::I8(0),
        Event::Str("int32"),
        Event::I32(21),
        Event::EndStruct,
        Event::StartStruct,
        Event::Str("int8"),
        Event::I8(1),
        Event::Str("int32"),
        Event::I32(42),
        Event::EndStruct,
        Event::EndSequence,
    ];
    assert_eq!(events, expected);

    let round_tripped: Vec<Example> = deserialize_from_source(&events)?;
    assert_eq!(&round_tripped, items);

    Ok(())
}

#[test]
fn example_options() -> Result<()> {
    #[derive(Serialize, Deserialize, PartialEq, Eq, Debug)]
    struct Example {
        int8: Option<i8>,
    }

    let items = &[Example { int8: Some(0) }, Example { int8: None }];

    let mut events = Vec::new();
    serialize_into_sink(&mut events, &items)?;
    let expected = vec![
        Event::StartSequence,
        Event::StartStruct,
        Event::Str("int8"),
        Event::Some,
        Event::I8(0),
        Event::EndStruct,
        Event::StartStruct,
        Event::Str("int8"),
        Event::Null,
        Event::EndStruct,
        Event::EndSequence,
    ];
    assert_eq!(events, expected);

    let round_tripped: Vec<Example> = deserialize_from_source(&events)?;
    assert_eq!(&round_tripped, items);

    Ok(())
}

#[test]
fn outer_struct() -> Result<()> {
    #[derive(Serialize, Deserialize, PartialEq, Eq, Debug)]
    struct Wrapper {
        ints: Vec<i8>,
        booleans: Vec<bool>,
    }

    let item = Wrapper {
        ints: vec![0, 1, 2],
        booleans: vec![true, false, true],
    };

    let mut events = Vec::new();
    serialize_into_sink(&mut events, &item)?;
    let expected = vec![
        Event::StartStruct,
        Event::Str("ints"),
        Event::StartSequence,
        Event::I8(0),
        Event::I8(1),
        Event::I8(2),
        Event::EndSequence,
        Event::Str("booleans"),
        Event::StartSequence,
        Event::Bool(true),
        Event::Bool(false),
        Event::Bool(true),
        Event::EndSequence,
        Event::EndStruct,
    ];
    assert_eq!(events, expected);

    let round_tripped: Wrapper = deserialize_from_source(&events)?;
    assert_eq!(round_tripped, item);

    Ok(())
}

#[test]
fn nested_struct() -> Result<()> {
    #[derive(Serialize, Deserialize, PartialEq, Eq, Debug)]
    struct Outer {
        int: i32,
        inner: Inner,
    }

    #[derive(Serialize, Deserialize, PartialEq, Eq, Debug)]
    struct Inner {
        int: i8,
        boolean: bool,
    }

    let items = vec![
        Outer {
            int: 0,
            inner: Inner {
                int: 1,
                boolean: true,
            },
        },
        Outer {
            int: 2,
            inner: Inner {
                int: 3,
                boolean: false,
            },
        },
    ];

    let mut events = Vec::new();
    serialize_into_sink(&mut events, &items)?;
    let expected = vec![
        Event::StartSequence,
        Event::StartStruct,
        Event::Str("int"),
        Event::I32(0),
        Event::Str("inner"),
        Event::StartStruct,
        Event::Str("int"),
        Event::I8(1),
        Event::Str("boolean"),
        Event::Bool(true),
        Event::EndStruct,
        Event::EndStruct,
        Event::StartStruct,
        Event::Str("int"),
        Event::I32(2),
        Event::Str("inner"),
        Event::StartStruct,
        Event::Str("int"),
        Event::I8(3),
        Event::Str("boolean"),
        Event::Bool(false),
        Event::EndStruct,
        Event::EndStruct,
        Event::EndSequence,
    ];
    assert_eq!(events, expected);

    let round_tripped: Vec<Outer> = deserialize_from_source(&events)?;
    assert_eq!(round_tripped, items);

    Ok(())
}
