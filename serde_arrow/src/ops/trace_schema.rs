use crate::{
    event::{serialize_into_sink, Event, EventSink},
    fail, DataType, Result, Schema,
};

use serde::Serialize;

/// Try to determine the schema from the existing records
///
/// This function inspects the individual records and tries to determine the
/// data types of each field. For most types, it is sufficient to trace a small
/// number of records to accurately determine the schema. For some fields no
/// data type can be determined, e.g., for options if all entries are missing.
/// In this case, the data type has to be overwritten manually via
/// [Schema::add_field]:
///
/// ```
/// # use std::convert::TryFrom;
/// # use serde_arrow::{Schema, DataType};
/// // Create a new TracedSchema
/// # let mut schema = Schema::new();
/// schema.add_field("col1", Some(DataType::I64), Some(true));
/// schema.add_field("col2", Some(DataType::I64), Some(false));
/// ```
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
    WaitForNullableValue(String),
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
        let mut state = State::Done;
        std::mem::swap(&mut self.state, &mut state);

        self.state = match (state, event) {
            (State::WaitForStartSequence, Event::StartSequence) => State::WaitForStartMap,
            (State::WaitForStartMap, Event::StartMap) => State::WaitForKey,
            (State::WaitForKey, Event::EndMap) => State::WaitForStartMap,
            (State::WaitForStartMap, Event::EndSequence) => State::Done,
            (State::WaitForKey, Event::Key(key)) => State::WaitForValue(key.to_owned()),
            (State::WaitForValue(key), Event::Some) => State::WaitForNullableValue(key),
            (State::WaitForValue(key), event) => {
                self.add_field(&key, event)?;
                State::WaitForKey
            }
            (State::WaitForNullableValue(key), event) => {
                self.add_field(&key, event)?;
                self.add_nullable(&key);
                State::WaitForKey
            }
            (state, event) => fail!("Unexpected event {} in state {:?}", event, state),
        };

        Ok(())
    }
}

impl SchemaSink {
    fn add_field(&mut self, key: &str, event: Event) -> Result<()> {
        match event {
            Event::Bool(_) => self.add_type(key, DataType::Bool),
            Event::I8(_) => self.add_type(key, DataType::I8),
            Event::I16(_) => self.add_type(key, DataType::I16),
            Event::I32(_) => self.add_type(key, DataType::I32),
            Event::I64(_) => self.add_type(key, DataType::I64),
            Event::U8(_) => self.add_type(key, DataType::U8),
            Event::U16(_) => self.add_type(key, DataType::U16),
            Event::U32(_) => self.add_type(key, DataType::U32),
            Event::U64(_) => self.add_type(key, DataType::U64),
            Event::F32(_) => self.add_type(key, DataType::F32),
            Event::F64(_) => self.add_type(key, DataType::F64),
            Event::Str(_) => self.add_type(key, DataType::Str),
            Event::Null => self.add_nullable(key),
            _ => fail!("Cannot add_field with event {}", event),
        }
        Ok(())
    }

    fn add_type(&mut self, key: &str, dt: DataType) {
        self.schema.add_field(key, Some(dt), None);
    }

    fn add_nullable(&mut self, key: &str) {
        self.schema.add_field(key, None, Some(true));
    }
}
