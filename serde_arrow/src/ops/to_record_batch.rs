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
        self.state = match (self.state, event) {
            (State::WaitForStartSequence, Event::StartSequence) => State::WaitForStartMap,
            (State::WaitForStartMap, Event::StartMap) => State::WaitForKey,
            (State::WaitForKey, Event::EndMap) => State::WaitForStartMap,
            (State::WaitForStartMap, Event::EndSequence) => State::Done,
            (State::WaitForKey, Event::Key(key)) => {
                let idx = self
                    .field_indices
                    .get(key)
                    .ok_or_else(|| error!("Unknown field {}", key))?;
                State::WaitForValue(*idx)
            }
            (State::WaitForValue(idx), Event::Some) => State::WaitForValue(idx),
            (State::WaitForValue(idx), event) => {
                self.append(idx, event)?;
                State::WaitForKey
            }
            (state, event) => fail!("Unexpected event {} in state {:?}", event, state),
        };
        Ok(())
    }
}

impl RecordBatchSink {
    fn append(&mut self, idx: usize, event: Event<'_>) -> Result<()> {
        match event {
            Event::Bool(val) => self.builders[idx].append_bool(val)?,
            Event::I8(val) => self.builders[idx].append_i8(val)?,
            Event::I16(val) => self.builders[idx].append_i16(val)?,
            Event::I32(val) => self.builders[idx].append_i32(val)?,
            Event::I64(val) => self.builders[idx].append_i64(val)?,
            Event::U8(val) => self.builders[idx].append_u8(val)?,
            Event::U16(val) => self.builders[idx].append_u16(val)?,
            Event::U32(val) => self.builders[idx].append_u32(val)?,
            Event::U64(val) => self.builders[idx].append_u64(val)?,
            Event::F32(val) => self.builders[idx].append_f32(val)?,
            Event::F64(val) => self.builders[idx].append_f64(val)?,
            Event::Str(val) => self.builders[idx].append_utf8(val)?,
            Event::Null => self.builders[idx].append_null()?,
            event => fail!("Cannot append event {}", event),
        }
        Ok(())
    }
}
