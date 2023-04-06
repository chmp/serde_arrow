use crate::_impl::arrow2::datatypes::{DataType, Field};
use serde::{Deserialize, Serialize};

use crate::{
    arrow2::{deserialize_from_arrays, serialize_into_arrays, serialize_into_fields},
    internal::{event::Event, sink::serialize_into_sink, source::deserialize_from_source},
};

#[test]
fn implementation_docs() {
    #[derive(Debug, PartialEq, Serialize, Deserialize)]
    struct Record {
        a: i32,
        b: u32,
    }

    let items = vec![
        Record { a: 1, b: 2 },
        Record { a: 3, b: 4 },
        // ...
    ];

    let mut events: Vec<Event<'static>> = Vec::new();
    serialize_into_sink(&mut events, &items).unwrap();

    assert_eq!(
        events,
        vec![
            Event::StartSequence,
            Event::StartStruct,
            Event::Str("a"),
            Event::I32(1),
            Event::Str("b"),
            Event::U32(2),
            Event::EndStruct,
            Event::StartStruct,
            Event::Str("a"),
            Event::I32(3),
            Event::Str("b"),
            Event::U32(4),
            Event::EndStruct,
            Event::EndSequence
        ],
    );

    let items_from_events: Vec<Record> = deserialize_from_source(&events).unwrap();
    assert_eq!(items_from_events, items);

    let fields = vec![
        Field::new("a", DataType::Int32, false),
        Field::new("b", DataType::UInt32, false),
    ];

    let arrays = serialize_into_arrays(&fields, &items).unwrap();
    let items_from_arrays: Vec<Record> = deserialize_from_arrays(&fields, &arrays).unwrap();

    assert_eq!(items_from_arrays, items);

    let fields_from_items = serialize_into_fields(&items, Default::default()).unwrap();
    assert_eq!(fields_from_items, fields);
}

#[test]
fn example_readme() -> Result<(), PanicOnError> {
    #[derive(Serialize)]
    struct Item {
        a: f32,
        b: i32,
        point: Point,
    }

    #[derive(Serialize)]
    struct Point(f32, f32);

    let items = vec![
        Item {
            a: 1.0,
            b: 1,
            point: Point(0.0, 1.0),
        },
        Item {
            a: 2.0,
            b: 2,
            point: Point(2.0, 3.0),
        },
        // ...
    ];

    // detect the field types and convert the items to arrays
    use crate::arrow2::{serialize_into_arrays, serialize_into_fields};

    let fields = serialize_into_fields(&items, Default::default())?;
    let arrays = serialize_into_arrays(&fields, &items)?;

    std::mem::drop((fields, arrays));
    Ok(())
}

#[derive(Debug)]
struct PanicOnError;

impl<E: std::error::Error> From<E> for PanicOnError {
    fn from(e: E) -> Self {
        panic!("Encountered error: {e}");
    }
}
