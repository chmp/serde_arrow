use crate::{
    event::{deserialize_from_source, Event},
    test::utils::TestSink,
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

    let events = TestSink::collect_events(&items)?;
    let expected = vec![
        Event::StartSequence,
        Event::StartMap,
        Event::owned_key("int8"),
        Event::I8(0),
        Event::owned_key("int32"),
        Event::I32(21),
        Event::EndMap,
        Event::StartMap,
        Event::owned_key("int8"),
        Event::I8(1),
        Event::owned_key("int32"),
        Event::I32(42),
        Event::EndMap,
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

    let events = TestSink::collect_events(&items)?;
    let expected = vec![
        Event::StartSequence,
        Event::StartMap,
        Event::owned_key("int8"),
        Event::Some,
        Event::I8(0),
        Event::EndMap,
        Event::StartMap,
        Event::owned_key("int8"),
        Event::Null,
        Event::EndMap,
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

    let events = TestSink::collect_events(&item)?;
    let expected = vec![
        Event::StartMap,
        Event::owned_key("ints"),
        Event::StartSequence,
        Event::I8(0),
        Event::I8(1),
        Event::I8(2),
        Event::EndSequence,
        Event::owned_key("booleans"),
        Event::StartSequence,
        Event::Bool(true),
        Event::Bool(false),
        Event::Bool(true),
        Event::EndSequence,
        Event::EndMap,
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

    let events = TestSink::collect_events(&items)?;
    let expected = vec![
        Event::StartSequence,
        Event::StartMap,
        Event::owned_key("int"),
        Event::I32(0),
        Event::owned_key("inner"),
        Event::StartMap,
        Event::owned_key("int"),
        Event::I8(1),
        Event::owned_key("boolean"),
        Event::Bool(true),
        Event::EndMap,
        Event::EndMap,
        Event::StartMap,
        Event::owned_key("int"),
        Event::I32(2),
        Event::owned_key("inner"),
        Event::StartMap,
        Event::owned_key("int"),
        Event::I8(3),
        Event::owned_key("boolean"),
        Event::Bool(false),
        Event::EndMap,
        Event::EndMap,
        Event::EndSequence,
    ];
    assert_eq!(events, expected);

    let round_tripped: Vec<Outer> = deserialize_from_source(&events)?;
    assert_eq!(round_tripped, items);

    Ok(())
}
