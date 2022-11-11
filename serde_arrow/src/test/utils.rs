use serde::Serialize;

use crate::{
    event::{serialize_into_sink, Event, EventSink},
    Result,
};

#[derive(Debug, Default)]
pub struct TestSink(Vec<Event<'static>>);

impl TestSink {
    pub fn new() -> Self {
        Self::default()
    }

    pub fn collect_events<T: Serialize + ?Sized>(value: &T) -> Result<Vec<Event<'static>>> {
        Ok(serialize_into_sink(Self::default(), value)?.into())
    }
}

impl From<TestSink> for Vec<Event<'static>> {
    fn from(sink: TestSink) -> Self {
        sink.0
    }
}

impl EventSink for TestSink {
    fn accept<'a>(&mut self, event: Event<'a>) -> Result<()> {
        match event {
            event => self.0.push(event.to_static()),
        }
        Ok(())
    }
}
