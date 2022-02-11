use crate::{event::Event, fail, DataType, Result, Schema};

use std::cell::Cell;

use arrow::{
    array::{
        BooleanArray, Date64Array, Float32Array, Float64Array, Int16Array, Int32Array, Int64Array,
        Int8Array, UInt16Array, UInt32Array, UInt64Array, UInt8Array,
    },
    datatypes::DataType as ArrowDataType,
    record_batch::RecordBatch,
};
use chrono::NaiveDateTime;

enum ArraySource<'a> {
    Bool(&'a BooleanArray),
    I8(&'a Int8Array),
    I16(&'a Int16Array),
    I32(&'a Int32Array),
    I64(&'a Int64Array),
    U8(&'a UInt8Array),
    U16(&'a UInt16Array),
    U32(&'a UInt32Array),
    U64(&'a UInt64Array),
    F32(&'a Float32Array),
    F64(&'a Float64Array),
    Date64NaiveDateTimeStr(&'a Date64Array),
}

impl<'a> ArraySource<'a> {
    fn emit<'this, 'event>(&'this self, idx: usize) -> Event<'event> {
        // TODO: handle nullability: arr.is_null(idx)
        match self {
            Self::Bool(arr) => arr.value(idx).into(),
            Self::I8(arr) => arr.value(idx).into(),
            Self::I16(arr) => arr.value(idx).into(),
            Self::I32(arr) => arr.value(idx).into(),
            Self::I64(arr) => arr.value(idx).into(),
            Self::U8(arr) => arr.value(idx).into(),
            Self::U16(arr) => arr.value(idx).into(),
            Self::U32(arr) => arr.value(idx).into(),
            Self::U64(arr) => arr.value(idx).into(),
            Self::F32(arr) => arr.value(idx).into(),
            Self::F64(arr) => arr.value(idx).into(),
            Self::Date64NaiveDateTimeStr(arr) => {
                let val = arr.value(idx);
                let val = NaiveDateTime::from_timestamp(val / 1000, (val % 1000) as u32 * 100_000);
                // NOTE: chrono documents that Debug can be parsed, Display cannot be parsed
                format!("{:?}", val).into()
            }
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
    pub fn new(record_batch: &'a RecordBatch, schema: &Schema) -> Result<Self> {
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
            let arrow_schema = record_batch.schema();
            let name = arrow_schema.field(i).name();
            let data_type = schema.data_type(name);
            let col = record_batch.column(i);

            let array_source = match col.data_type() {
                ArrowDataType::Boolean => ArraySource::Bool(col.as_any().downcast_ref().unwrap()),
                ArrowDataType::Int8 => {
                    ArraySource::I8(col.as_any().downcast_ref::<Int8Array>().unwrap())
                }
                ArrowDataType::Int16 => {
                    ArraySource::I16(col.as_any().downcast_ref::<Int16Array>().unwrap())
                }
                ArrowDataType::Int32 => {
                    ArraySource::I32(col.as_any().downcast_ref::<Int32Array>().unwrap())
                }
                ArrowDataType::Int64 => {
                    ArraySource::I64(col.as_any().downcast_ref::<Int64Array>().unwrap())
                }
                ArrowDataType::UInt8 => {
                    ArraySource::U8(col.as_any().downcast_ref::<UInt8Array>().unwrap())
                }
                ArrowDataType::UInt16 => {
                    ArraySource::U16(col.as_any().downcast_ref::<UInt16Array>().unwrap())
                }
                ArrowDataType::UInt32 => {
                    ArraySource::U32(col.as_any().downcast_ref::<UInt32Array>().unwrap())
                }
                ArrowDataType::UInt64 => {
                    ArraySource::U64(col.as_any().downcast_ref::<UInt64Array>().unwrap())
                }
                ArrowDataType::Float32 => {
                    ArraySource::F32(col.as_any().downcast_ref::<Float32Array>().unwrap())
                }
                ArrowDataType::Float64 => {
                    ArraySource::F64(col.as_any().downcast_ref::<Float64Array>().unwrap())
                }
                ArrowDataType::Date32 => match data_type {
                    Some(DataType::DateTimeMilliseconds) => todo!(),
                    Some(DataType::NaiveDateTimeStr) => todo!(),
                    Some(DataType::DateTimeStr) => todo!(),
                    Some(dt) => fail!("Annotation {} is not supported by Date32", dt),
                    None => fail!("Date32 columns require additional data type annotations"),
                },
                ArrowDataType::Date64 => match data_type {
                    Some(DataType::DateTimeMilliseconds) => todo!(),
                    Some(DataType::NaiveDateTimeStr) => ArraySource::Date64NaiveDateTimeStr(
                        col.as_any().downcast_ref::<Date64Array>().unwrap(),
                    ),
                    Some(DataType::DateTimeStr) => todo!(),
                    Some(dt) => fail!("Annotation {} is not supported by Date64", dt),
                    None => fail!("Date64 columns require additional data type annotations"),
                },
                dt => fail!("Arrow DataType {} not understood", dt),
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
