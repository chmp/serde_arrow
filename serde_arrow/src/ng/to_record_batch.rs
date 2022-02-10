use crate::{
    error,
    event::{serialize_into_sink, Event, EventSink},
    fail,
    util::array_builder::ArrayBuilder,
    Result, Schema,
};

use std::{collections::HashMap, sync::Arc};

use arrow::{array::ArrayRef, datatypes::Schema as ArrowSchema, record_batch::RecordBatch};
use serde::Serialize;

/// Convert a sequence of records into an Arrow RecordBatch
///
pub fn to_record_batch<T>(value: &T, schema: &Schema) -> Result<RecordBatch>
where
    T: Serialize + ?Sized,
{
    serialize_into_sink(RecordBatchSink::new(schema)?, value)?.build()
}

struct RecordBatchSink {
    state: State,
    schema: ArrowSchema,
    field_indices: HashMap<String, usize>,
    builders: Vec<ArrayBuilder>,
}

#[derive(Debug, Clone, Copy)]
enum State {
    WaitForStartSequence,
    WaitForStartMap,
    WaitForKey,
    WaitForValue(usize),
    Done,
}

impl RecordBatchSink {
    fn new(schema: &Schema) -> Result<Self> {
        let mut field_indices = HashMap::new();
        let mut builders = Vec::new();

        for (idx, field) in schema.fields().iter().enumerate() {
            field_indices.insert(field.to_owned(), idx);
            let dt = schema
                .data_type(field)
                .ok_or_else(|| error!("No known data type for field {}", field))?;
            builders.push(ArrayBuilder::new(dt)?);
        }

        let res = Self {
            state: State::WaitForStartSequence,
            schema: schema.build_arrow_schema()?,
            field_indices,
            builders,
        };
        Ok(res)
    }

    fn build(self) -> Result<RecordBatch> {
        let mut fields: Vec<ArrayRef> = Vec::new();

        for mut builder in self.builders {
            fields.push(builder.build()?);
        }

        let res = RecordBatch::try_new(Arc::new(self.schema), fields)?;
        Ok(res)
    }
}

impl EventSink for RecordBatchSink {
    fn accept(&mut self, event: Event<'_>) -> Result<()> {
        match (self.state, event) {
            (State::WaitForStartSequence, Event::StartSequence) => {
                self.state = State::WaitForStartMap;
            }
            (State::WaitForStartMap, Event::StartMap) => {
                self.state = State::WaitForKey;
            }
            (State::WaitForKey, Event::EndMap) => {
                self.state = State::WaitForStartMap;
            }
            (State::WaitForStartMap, Event::EndSequence) => {
                self.state = State::Done;
            }
            (State::WaitForKey, Event::Key(key)) => {
                let idx = self
                    .field_indices
                    .get(key)
                    .ok_or_else(|| error!("Unknown field {}", key))?;
                self.state = State::WaitForValue(*idx);
            }
            (State::WaitForValue(idx), Event::I8(val)) => {
                self.builders[idx].append_i8(val)?;
                self.state = State::WaitForKey;
            }
            (State::WaitForValue(idx), Event::I32(val)) => {
                self.builders[idx].append_i32(val)?;
                self.state = State::WaitForKey;
            }
            (state, event) => fail!("Unexpected event {} in state {:?}", event, state),
        }
        Ok(())
    }
}
