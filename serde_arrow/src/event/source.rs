use crate::{event::Event, Result};

use std::cell::Cell;

use arrow::{
    array::{Int32Array, Int8Array},
    datatypes::DataType,
    record_batch::RecordBatch,
};

enum ArraySource<'a> {
    I8(&'a Int8Array),
    I32(&'a Int32Array),
}

impl<'a> ArraySource<'a> {
    fn emit<'this, 'event>(&'this self, idx: usize) -> Event<'event> {
        // TODO: handle nullability: arr.is_null(idx)
        match self {
            Self::I8(arr) => arr.value(idx).into(),
            Self::I32(arr) => arr.value(idx).into(),
        }
    }
}

pub struct RecordBatchSource<'a> {
    num_rows: usize,
    num_columns: usize,
    columns: Vec<String>,
    state: Cell<State>,
    array_sources: Vec<ArraySource<'a>>,
}

#[derive(Debug, Clone, Copy)]
enum State {
    StartSequence,
    StartMap(usize),
    Key(usize, usize),
    Value(usize, usize),
    Done,
}

impl<'a> RecordBatchSource<'a> {
    pub fn new(record_batch: &'a RecordBatch) -> Result<Self> {
        let num_rows = record_batch.num_rows();
        let num_columns = record_batch.num_columns();
        let columns = record_batch
            .schema()
            .fields()
            .iter()
            .map(|f| f.name().to_owned())
            .collect();
        let state = Cell::new(State::StartSequence);

        let mut array_sources = Vec::new();

        for i in 0..num_columns {
            let col = record_batch.column(i);
            let array_source = match col.data_type() {
                DataType::Int8 => {
                    ArraySource::I8(col.as_any().downcast_ref::<Int8Array>().unwrap())
                }
                DataType::Int32 => {
                    ArraySource::I32(col.as_any().downcast_ref::<Int32Array>().unwrap())
                }
                _ => todo!(),
            };
            array_sources.push(array_source);
        }

        let res = Self {
            num_rows,
            num_columns,
            columns,
            state,
            array_sources,
        };
        // TODO: validate
        Ok(res)
    }

    pub fn is_done(&self) -> bool {
        matches!(self.state.get(), State::Done)
    }

    pub fn peek(&self) -> Option<Event<'_>> {
        match self.state.get() {
            State::StartSequence => Some(Event::StartSequence),
            State::StartMap(row) if row >= self.num_rows => Some(Event::EndSequence),
            State::StartMap(_) => Some(Event::StartMap),
            State::Key(_, col) if col >= self.num_columns => Some(Event::EndMap),
            State::Key(_, col) => Some(Event::Key(&self.columns[col])),
            State::Value(row, col) => Some(self.array_sources[col].emit(row)),
            State::Done => None,
        }
    }

    fn next_state(&self) -> Option<State> {
        match self.state.get() {
            State::StartSequence => Some(State::StartMap(0)),
            State::StartMap(row) if row >= self.num_rows => Some(State::Done),
            State::StartMap(row) => Some(State::Key(row, 0)),
            State::Key(row, col) if col >= self.num_columns => Some(State::StartMap(row + 1)),
            State::Key(row, col) => Some(State::Value(row, col)),
            State::Value(row, col) => Some(State::Key(row, col + 1)),
            State::Done => None,
        }
    }

    pub fn next(&mut self) -> Event<'_> {
        let next_event = self
            .peek()
            .expect("Invalid call to next on exhausted EventSource");
        let next_state = self.next_state().unwrap();
        self.state.set(next_state);
        next_event
    }
}
