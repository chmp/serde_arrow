use crate::{
    event::{serialize_into_sink, Event, EventSink},
    Result,
};
use serde::{Deserialize, Serialize};

#[test]
fn example() -> Result<()> {
    let original = &[
        Example { int8: 0, int32: 21 },
        Example { int8: 1, int32: 42 },
    ];

    let sink = serialize_into_sink(TestSink::default(), &original)?;
    let events = sink.0;

    assert_eq!(
        events,
        vec![
            String::from("StartSequence"),
            String::from("StartMap"),
            String::from("Key:int8"),
            String::from("I8"),
            String::from("Key:int32"),
            String::from("I32"),
            String::from("EndMap"),
            String::from("StartMap"),
            String::from("Key:int8"),
            String::from("I8"),
            String::from("Key:int32"),
            String::from("I32"),
            String::from("EndMap"),
            String::from("EndSequence"),
        ]
    );
    Ok(())
}

#[derive(Serialize, Deserialize, PartialEq, Eq, Debug)]
struct Example {
    int8: i8,
    int32: i32,
}

#[derive(Debug, Default)]
struct TestSink(Vec<String>);

impl EventSink for TestSink {
    fn accept<'a>(&mut self, event: Event<'a>) -> Result<()> {
        match event {
            Event::Key(name) => self.0.push(format!("Key:{}", name)),
            event => self.0.push(event.to_string()),
        }
        Ok(())
    }
}
