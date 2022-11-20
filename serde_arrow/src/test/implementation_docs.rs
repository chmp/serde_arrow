use arrow2::datatypes::{DataType, Field};
use serde::{Deserialize, Serialize};

use crate::{
    arrow2::{deserialize_from_arrays, serialize_into_arrays, serialize_into_fields},
    base::{deserialize_from_source, serialize_into_sink, Event},
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
            Event::StartMap,
            Event::OwnedKey(String::from("a")),
            Event::I32(1),
            Event::OwnedKey(String::from("b")),
            Event::U32(2),
            Event::EndMap,
            Event::StartMap,
            Event::OwnedKey(String::from("a")),
            Event::I32(3),
            Event::OwnedKey(String::from("b")),
            Event::U32(4),
            Event::EndMap,
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

    let fields_from_items = serialize_into_fields(&items).unwrap();
    assert_eq!(fields_from_items, fields);
}
