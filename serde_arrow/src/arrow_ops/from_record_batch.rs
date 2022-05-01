use crate::event::Source;
use crate::{
    error,
    event::{self, Event},
    fail, DataType, Result, Schema,
};

use std::cell::Cell;

use arrow::{
    array::{
        Array, BooleanArray, Date64Array, Float32Array, Float64Array, Int16Array, Int32Array,
        Int64Array, Int8Array, LargeStringArray, PrimitiveArray, StringArray, UInt16Array,
        UInt32Array, UInt64Array, UInt8Array,
    },
    datatypes::{ArrowPrimitiveType, DataType as ArrowDataType},
    record_batch::RecordBatch,
};
use chrono::{NaiveDateTime, TimeZone, Utc};

use serde::Deserialize;

pub fn from_record_batch<'de, T: Deserialize<'de>>(
    record_batch: &'de RecordBatch,
    schema: &Schema,
) -> Result<T> {
    let source = RecordBatchSource::new(record_batch, schema)?;
    event::from_source(source)
}

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
    Utf8(&'a StringArray),
    LargeUtf8(&'a LargeStringArray),
    Date64NaiveDateTimeStr(&'a Date64Array),
    Date64DateTimeStr(&'a Date64Array),
    Date64DateTimeMilliseconds(&'a Date64Array),
}

impl<'a> ArraySource<'a> {
    fn emit<'this, 'event>(&'this self, idx: usize) -> Event<'event> {
        fn emit_primitive<'this, 'event, T>(
            arr: &'this PrimitiveArray<T>,
            idx: usize,
        ) -> Event<'event>
        where
            T: ArrowPrimitiveType,
            T::Native: Into<Event<'event>>,
        {
            if arr.is_null(idx) {
                Event::Null
            } else {
                arr.value(idx).into()
            }
        }

        match self {
            Self::Bool(arr) => {
                if arr.is_null(idx) {
                    Event::Null
                } else {
                    arr.value(idx).into()
                }
            }
            Self::I8(arr) => emit_primitive(arr, idx),
            Self::I16(arr) => emit_primitive(arr, idx),
            Self::I32(arr) => emit_primitive(arr, idx),
            Self::I64(arr) => emit_primitive(arr, idx),
            Self::U8(arr) => emit_primitive(arr, idx),
            Self::U16(arr) => emit_primitive(arr, idx),
            Self::U32(arr) => emit_primitive(arr, idx),
            Self::U64(arr) => emit_primitive(arr, idx),
            Self::F32(arr) => emit_primitive(arr, idx),
            Self::F64(arr) => emit_primitive(arr, idx),
            Self::Utf8(arr) => {
                if arr.is_null(idx) {
                    Event::Null
                } else {
                    // TODO: can this be done zero copy?
                    arr.value(idx).to_owned().into()
                }
            }
            Self::LargeUtf8(arr) => {
                if arr.is_null(idx) {
                    Event::Null
                } else {
                    // TODO: can this be done zero copy?
                    arr.value(idx).to_owned().into()
                }
            }
            Self::Date64DateTimeMilliseconds(arr) => emit_primitive(arr, idx),
            Self::Date64NaiveDateTimeStr(arr) => {
                if arr.is_null(idx) {
                    Event::Null
                } else {
                    let val = arr.value(idx);
                    let val =
                        NaiveDateTime::from_timestamp(val / 1000, (val % 1000) as u32 * 100_000);
                    // NOTE: chrono documents that Debug, not Display, can be parsed
                    format!("{:?}", val).into()
                }
            }
            Self::Date64DateTimeStr(arr) => {
                if arr.is_null(idx) {
                    Event::Null
                } else {
                    let val = arr.value(idx);
                    let val = Utc.timestamp(val / 1000, (val % 1000) as u32 * 100_000);
                    // NOTE: chrono documents that Debug, not Display, can be parsed
                    format!("{:?}", val).into()
                }
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
                ArrowDataType::Int8 => ArraySource::I8(col.as_any().downcast_ref().unwrap()),
                ArrowDataType::Int16 => ArraySource::I16(col.as_any().downcast_ref().unwrap()),
                ArrowDataType::Int32 => ArraySource::I32(col.as_any().downcast_ref().unwrap()),
                ArrowDataType::Int64 => ArraySource::I64(col.as_any().downcast_ref().unwrap()),
                ArrowDataType::UInt8 => ArraySource::U8(col.as_any().downcast_ref().unwrap()),
                ArrowDataType::UInt16 => ArraySource::U16(col.as_any().downcast_ref().unwrap()),
                ArrowDataType::UInt32 => ArraySource::U32(col.as_any().downcast_ref().unwrap()),
                ArrowDataType::UInt64 => ArraySource::U64(col.as_any().downcast_ref().unwrap()),
                ArrowDataType::Float32 => ArraySource::F32(col.as_any().downcast_ref().unwrap()),
                ArrowDataType::Float64 => ArraySource::F64(col.as_any().downcast_ref().unwrap()),
                ArrowDataType::Utf8 => ArraySource::Utf8(col.as_any().downcast_ref().unwrap()),
                ArrowDataType::LargeUtf8 => {
                    ArraySource::LargeUtf8(col.as_any().downcast_ref().unwrap())
                }
                ArrowDataType::Date32 => fail!("Date32 are not supported at the moment"),
                ArrowDataType::Date64 => match data_type {
                    Some(DataType::DateTimeMilliseconds) => {
                        ArraySource::Date64DateTimeMilliseconds(
                            col.as_any().downcast_ref().unwrap(),
                        )
                    }
                    Some(DataType::NaiveDateTimeStr) => {
                        ArraySource::Date64NaiveDateTimeStr(col.as_any().downcast_ref().unwrap())
                    }
                    Some(DataType::DateTimeStr) => {
                        ArraySource::Date64DateTimeStr(col.as_any().downcast_ref().unwrap())
                    }
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
        Ok(res)
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
}

impl<'a> Source for RecordBatchSource<'a> {
    fn is_done(&self) -> bool {
        matches!(self.state.get(), State::Done)
    }

    fn peek(&self) -> Option<Event<'_>> {
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

    fn next_event(&mut self) -> Result<Event<'_>> {
        let next_event = self
            .peek()
            .ok_or_else(|| error!("Invalid call to next on exhausted EventSource"))?;
        let next_state = self.next_state().expect("next_event: Inconsistent state");
        self.state.set(next_state);
        Ok(next_event)
    }
}

#[cfg(test)]
mod test {
    use super::*;

    use serde::Serialize;

    #[test]
    fn event_source() -> Result<()> {
        #[derive(Serialize, PartialEq, Eq, Debug)]
        struct Example {
            int8: i8,
            int32: i32,
        }

        let original = &[
            Example { int8: 0, int32: 21 },
            Example { int8: 1, int32: 42 },
        ];
        let schema = Schema::from_records(&original)?;
        let record_batch = crate::to_record_batch(&original, &schema)?;

        let mut event_source = RecordBatchSource::new(&record_batch, &schema)?;

        assert_eq!(event_source.next_event()?, Event::StartSequence);
        assert_eq!(event_source.next_event()?, Event::StartMap);
        assert_eq!(event_source.next_event()?, Event::Key("int8"));
        assert_eq!(event_source.next_event()?, Event::I8(0));
        assert_eq!(event_source.next_event()?, Event::Key("int32"));
        assert_eq!(event_source.next_event()?, Event::I32(21));
        assert_eq!(event_source.next_event()?, Event::EndMap);
        assert_eq!(event_source.next_event()?, Event::StartMap);
        assert_eq!(event_source.next_event()?, Event::Key("int8"));
        assert_eq!(event_source.next_event()?, Event::I8(1));
        assert_eq!(event_source.next_event()?, Event::Key("int32"));
        assert_eq!(event_source.next_event()?, Event::I32(42));
        assert_eq!(event_source.next_event()?, Event::EndMap);
        assert_eq!(event_source.next_event()?, Event::EndSequence);

        Ok(())
    }
}
