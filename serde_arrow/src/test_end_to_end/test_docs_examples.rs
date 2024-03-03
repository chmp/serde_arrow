use serde::{Deserialize, Serialize};

use crate::internal::{event::Event, sink::serialize_into_sink, source::deserialize_from_source};

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
            Event::Item,
            Event::StartStruct,
            Event::Str("a"),
            Event::I32(1),
            Event::Str("b"),
            Event::U32(2),
            Event::EndStruct,
            Event::Item,
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
}