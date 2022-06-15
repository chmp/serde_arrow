use std::collections::HashMap;

use serde::Serialize;

use crate::{
    error,
    event::{serialize_into_sink, Event, EventSink},
    fail, DataType, Result, Schema,
};

/// Convert a sequence of records into an Arrow RecordBatch
///
pub fn to_arrays<A: ArrayBuilder, T>(value: &T, schema: &Schema) -> Result<Vec<A::ArrayRef>>
where
    T: Serialize + ?Sized,
{
    let sink = RecordBatchSink::<A>::new(schema)?;
    let sink = serialize_into_sink(sink, value)?;
    let arrays = sink.build_arrays()?;
    Ok(arrays)
}

pub trait ArrayBuilder: Sized {
    type ArrayRef: Sized;

    fn new(data_type: &DataType) -> Result<Self>;
    fn build(&mut self) -> Result<Self::ArrayRef>;

    fn append_null(&mut self) -> Result<()>;
    fn append_bool(&mut self, value: bool) -> Result<()>;
    fn append_i8(&mut self, value: i8) -> Result<()>;
    fn append_i16(&mut self, value: i16) -> Result<()>;
    fn append_i32(&mut self, value: i32) -> Result<()>;
    fn append_i64(&mut self, value: i64) -> Result<()>;
    fn append_u8(&mut self, value: u8) -> Result<()>;
    fn append_u16(&mut self, value: u16) -> Result<()>;
    fn append_u32(&mut self, value: u32) -> Result<()>;
    fn append_u64(&mut self, value: u64) -> Result<()>;
    fn append_f32(&mut self, value: f32) -> Result<()>;
    fn append_f64(&mut self, value: f64) -> Result<()>;
    fn append_utf8(&mut self, data: &str) -> Result<()>;
}

struct RecordBatchSink<A> {
    state: State,
    field_indices: HashMap<String, usize>,
    builders: Vec<A>,
}

#[derive(Debug, Clone, Copy)]
enum State {
    WaitForStartSequence,
    WaitForStartMap,
    WaitForKey,
    WaitForValue(usize),
    Done,
}

impl<A: ArrayBuilder> RecordBatchSink<A> {
    fn new(schema: &Schema) -> Result<Self> {
        let mut field_indices = HashMap::new();
        let mut builders = Vec::new();

        for (idx, field) in schema.fields().iter().enumerate() {
            field_indices.insert(field.to_owned(), idx);
            let dt = schema
                .data_type(field)
                .ok_or_else(|| error!("No known data type for field {}", field))?;
            builders.push(A::new(dt)?);
        }

        let res = Self {
            state: State::WaitForStartSequence,
            field_indices,
            builders,
        };
        Ok(res)
    }

    fn build_arrays(self) -> Result<Vec<A::ArrayRef>> {
        let mut arrays: Vec<A::ArrayRef> = Vec::new();
        for mut builder in self.builders {
            arrays.push(builder.build()?);
        }

        Ok(arrays)
    }
}

impl<A: ArrayBuilder> EventSink for RecordBatchSink<A> {
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

impl<A: ArrayBuilder> RecordBatchSink<A> {
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
