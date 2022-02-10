use crate::{
    event::{serialize_into_sink, Event, EventSink},
    fail, DataType, Result, Schema,
};

use serde::Serialize;

pub fn trace_schema<T>(value: &T) -> Result<Schema>
where
    T: Serialize + ?Sized,
{
    let sink = serialize_into_sink(SchemaSink::new(), value)?;
    Ok(sink.schema)
}

struct SchemaSink {
    schema: Schema,
    state: State,
}

#[derive(Debug)]
enum State {
    WaitForStartSequence,
    WaitForStartMap,
    WaitForKey,
    WaitForValue(String),
    Done,
}

impl SchemaSink {
    fn new() -> Self {
        Self {
            schema: Schema::new(),
            state: State::WaitForStartSequence,
        }
    }
}

impl EventSink for SchemaSink {
    fn accept(&mut self, event: Event<'_>) -> Result<()> {
        match (event, &self.state) {
            (Event::StartSequence, State::WaitForStartSequence) => {
                self.state = State::WaitForStartMap;
            }
            (Event::StartMap, State::WaitForStartMap) => {
                self.state = State::WaitForKey;
            }
            (Event::EndMap, State::WaitForKey) => {
                self.state = State::WaitForStartMap;
            }
            (Event::EndSequence, State::WaitForStartMap) => {
                self.state = State::Done;
            }
            (Event::Key(key), State::WaitForKey) => {
                self.state = State::WaitForValue(key.to_owned());
            }
            (Event::I8(_), State::WaitForValue(key)) => {
                self.schema.add_field(key, Some(DataType::I8), None);
                self.state = State::WaitForKey;
            }
            (Event::I32(_), State::WaitForValue(key)) => {
                self.schema.add_field(key, Some(DataType::I32), None);
                self.state = State::WaitForKey;
            }
            (event, state) => fail!("Unexpected event {} in state {:?}", event, state),
        }
        Ok(())
    }
}
