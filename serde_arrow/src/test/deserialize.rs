#![allow(unused)]
use crate::{error, fail, Error, Result};

use std::cell::Cell;

use arrow::{
    array::{Int32Array, Int8Array},
    datatypes::DataType,
    record_batch::RecordBatch,
};
use serde::de::{self, DeserializeSeed, MapAccess, SeqAccess, Visitor};
use serde::{Deserialize, Serialize};

#[test]
fn event_source() -> Result<()> {
    let original = &[
        Example { int8: 0, int32: 21 },
        Example { int8: 1, int32: 42 },
    ];
    let schema = crate::trace_schema(&original)?;
    let record_batch = crate::to_record_batch(&original, &schema)?;

    let mut event_source = RecordBatchSource::new(&record_batch)?;

    assert_eq!(event_source.next(), Event::StartSequence);
    assert_eq!(event_source.next(), Event::StartMap);
    assert_eq!(event_source.next(), Event::Key("int8"));
    assert_eq!(event_source.next(), Event::Int8(0));
    assert_eq!(event_source.next(), Event::Key("int32"));
    assert_eq!(event_source.next(), Event::Int32(21));
    assert_eq!(event_source.next(), Event::EndMap);
    assert_eq!(event_source.next(), Event::StartMap);
    assert_eq!(event_source.next(), Event::Key("int8"));
    assert_eq!(event_source.next(), Event::Int8(1));
    assert_eq!(event_source.next(), Event::Key("int32"));
    assert_eq!(event_source.next(), Event::Int32(42));
    assert_eq!(event_source.next(), Event::EndMap);
    assert_eq!(event_source.next(), Event::EndSequence);

    Ok(())
}

#[test]
fn example() -> Result<()> {
    let original = &[
        Example { int8: 0, int32: 21 },
        Example { int8: 1, int32: 42 },
    ];
    let schema = crate::trace_schema(&original)?;
    let record_batch = crate::to_record_batch(&original, &schema)?;
    let round_tripped = from_record_batch::<Vec<Example>>(&record_batch)?;

    assert_eq!(round_tripped, original);

    Ok(())
}

#[derive(Serialize, Deserialize, PartialEq, Eq, Debug)]
struct Example {
    int8: i8,
    int32: i32,
}

fn from_record_batch<'de, T: Deserialize<'de>>(record_batch: &'de RecordBatch) -> Result<T> {
    let mut deserializer = Deserializer::from_record_batch(&record_batch)?;
    let res = T::deserialize(&mut deserializer)?;

    if !deserializer.is_done() {
        fail!("Trailing content");
    }

    Ok(res)
}

#[derive(Debug, PartialEq)]
enum Event<'a> {
    StartSequence,
    StartMap,
    Key(&'a str),
    Int8(i8),
    Int32(i32),
    EndMap,
    EndSequence,
}

impl<'a> std::fmt::Display for Event<'a> {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Event::StartSequence => write!(f, "StartSequence"),
            Event::StartMap => write!(f, "StartMap"),
            Event::Key(_) => write!(f, "Key"),
            Event::Int8(_) => write!(f, "Int8"),
            Event::Int32(_) => write!(f, "Int32"),
            Event::EndMap => write!(f, "EndMap"),
            Event::EndSequence => write!(f, "EndSequence"),
        }
    }
}

impl<'a> From<i8> for Event<'a> {
    fn from(val: i8) -> Self {
        Self::Int8(val)
    }
}

impl<'a> From<i32> for Event<'a> {
    fn from(val: i32) -> Self {
        Self::Int32(val)
    }
}

enum ArraySource<'a> {
    Int8(&'a Int8Array),
    Int32(&'a Int32Array),
}

impl<'a> ArraySource<'a> {
    fn emit<'this, 'event>(&'this self, idx: usize) -> Event<'event> {
        // TODO: handle nullability: arr.is_null(idx)
        match self {
            Self::Int8(arr) => arr.value(idx).into(),
            Self::Int32(arr) => arr.value(idx).into(),
        }
    }
}

struct RecordBatchSource<'a> {
    record_batch: &'a RecordBatch,
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
    // TODO: accept a schema
    fn new(record_batch: &'a RecordBatch) -> Result<Self> {
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
                    ArraySource::Int8(col.as_any().downcast_ref::<Int8Array>().unwrap())
                }
                DataType::Int32 => {
                    ArraySource::Int32(col.as_any().downcast_ref::<Int32Array>().unwrap())
                }
                _ => todo!(),
            };
            array_sources.push(array_source);
        }

        let res = Self {
            record_batch,
            num_rows,
            num_columns,
            columns,
            state,
            array_sources,
        };
        // TODO: validate
        Ok(res)
    }

    fn is_done(&self) -> bool {
        matches!(self.state.get(), State::Done)
    }

    fn peek(&self) -> Option<Event<'_>> {
        match self.state.get() {
            State::StartSequence => Some(Event::StartSequence),
            State::StartMap(row) if row >= self.num_rows => Some(Event::EndSequence),
            State::StartMap(row) => Some(Event::StartMap),
            State::Key(row, col) if col >= self.num_columns => Some(Event::EndMap),
            State::Key(row, col) => Some(Event::Key(&self.columns[col])),
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

    // TODO: remove code duplication
    fn next(&mut self) -> Event<'_> {
        let next_event = self
            .peek()
            .expect("Invalid call to next on exhausted EventSource");
        let next_state = self.next_state().unwrap();
        self.state.set(next_state);
        next_event
    }
}

pub struct Deserializer<'de> {
    event_source: RecordBatchSource<'de>,
}

impl<'de> Deserializer<'de> {
    fn from_record_batch(record_batch: &'de RecordBatch) -> Result<Self> {
        let res = Self {
            event_source: RecordBatchSource::new(record_batch)?,
        };
        Ok(res)
    }

    fn is_done(&self) -> bool {
        self.event_source.is_done()
    }
}

impl<'de, 'a> de::Deserializer<'de> for &'a mut Deserializer<'de> {
    type Error = Error;

    fn deserialize_any<V: Visitor<'de>>(self, visitor: V) -> Result<V::Value> {
        todo!()
    }

    fn deserialize_bool<V: Visitor<'de>>(self, visitor: V) -> Result<V::Value> {
        todo!()
    }

    fn deserialize_i8<V: Visitor<'de>>(self, visitor: V) -> Result<V::Value> {
        match self.event_source.next() {
            Event::Int8(v) => visitor.visit_i8(v),
            ev => fail!("Expected i8, found {}", ev),
        }
    }

    fn deserialize_i16<V: Visitor<'de>>(self, visitor: V) -> Result<V::Value> {
        todo!()
    }

    fn deserialize_i32<V: Visitor<'de>>(self, visitor: V) -> Result<V::Value> {
        match self.event_source.next() {
            Event::Int32(v) => visitor.visit_i32(v),
            ev => fail!("Expected i8, found {}", ev),
        }
    }

    fn deserialize_i64<V: Visitor<'de>>(self, visitor: V) -> Result<V::Value> {
        todo!()
    }

    fn deserialize_u8<V: Visitor<'de>>(self, visitor: V) -> Result<V::Value> {
        todo!()
    }

    fn deserialize_u16<V: Visitor<'de>>(self, visitor: V) -> Result<V::Value> {
        todo!()
    }

    fn deserialize_u32<V: Visitor<'de>>(self, visitor: V) -> Result<V::Value> {
        todo!()
    }

    fn deserialize_u64<V: Visitor<'de>>(self, visitor: V) -> Result<V::Value> {
        todo!()
    }

    fn deserialize_f32<V: Visitor<'de>>(self, _visitor: V) -> Result<V::Value> {
        todo!()
    }

    fn deserialize_f64<V: Visitor<'de>>(self, _visitor: V) -> Result<V::Value> {
        todo!()
    }

    fn deserialize_char<V: Visitor<'de>>(self, _visitor: V) -> Result<V::Value> {
        todo!()
    }

    fn deserialize_str<V: Visitor<'de>>(self, visitor: V) -> Result<V::Value> {
        match self.event_source.next() {
            Event::Key(key) => visitor.visit_str(key),
            ev => fail!("Invalid event {}, expected str", ev),
        }
    }

    fn deserialize_string<V: Visitor<'de>>(self, visitor: V) -> Result<V::Value> {
        todo!()
    }

    fn deserialize_bytes<V: Visitor<'de>>(self, _visitor: V) -> Result<V::Value> {
        todo!()
    }

    fn deserialize_byte_buf<V: Visitor<'de>>(self, _visitor: V) -> Result<V::Value> {
        todo!()
    }

    fn deserialize_option<V: Visitor<'de>>(self, visitor: V) -> Result<V::Value> {
        todo!()
    }

    fn deserialize_unit<V: Visitor<'de>>(self, visitor: V) -> Result<V::Value> {
        todo!()
    }

    fn deserialize_unit_struct<V: Visitor<'de>>(
        self,
        _name: &'static str,
        visitor: V,
    ) -> Result<V::Value> {
        todo!()
    }

    fn deserialize_newtype_struct<V: Visitor<'de>>(
        self,
        _name: &'static str,
        visitor: V,
    ) -> Result<V::Value> {
        todo!()
    }

    fn deserialize_seq<V: Visitor<'de>>(self, visitor: V) -> Result<V::Value> {
        if !matches!(self.event_source.next(), Event::StartSequence) {
            fail!("Expected start of sequence");
        }

        let res = visitor.visit_seq(&mut *self)?;

        if !matches!(self.event_source.next(), Event::EndSequence) {
            fail!("Expected end of sequence");
        }
        Ok(res)
    }

    fn deserialize_tuple<V: Visitor<'de>>(self, _len: usize, visitor: V) -> Result<V::Value> {
        self.deserialize_seq(visitor)
    }

    fn deserialize_tuple_struct<V: Visitor<'de>>(
        self,
        _name: &'static str,
        _len: usize,
        visitor: V,
    ) -> Result<V::Value> {
        self.deserialize_seq(visitor)
    }

    fn deserialize_map<V: Visitor<'de>>(self, visitor: V) -> Result<V::Value> {
        if !matches!(self.event_source.next(), Event::StartMap) {
            fail!("Expected start of map");
        }

        let res = visitor.visit_map(&mut *self)?;

        if !matches!(self.event_source.next(), Event::EndMap) {
            fail!("Expected end of map");
        }
        Ok(res)
    }

    fn deserialize_struct<V: Visitor<'de>>(
        self,
        _name: &'static str,
        _fields: &'static [&'static str],
        visitor: V,
    ) -> Result<V::Value> {
        self.deserialize_map(visitor)
    }

    fn deserialize_enum<V: Visitor<'de>>(
        self,
        _name: &'static str,
        _variants: &'static [&'static str],
        visitor: V,
    ) -> Result<V::Value> {
        todo!()
    }

    fn deserialize_identifier<V: Visitor<'de>>(self, visitor: V) -> Result<V::Value> {
        self.deserialize_str(visitor)
    }

    fn deserialize_ignored_any<V: Visitor<'de>>(self, visitor: V) -> Result<V::Value> {
        self.deserialize_any(visitor)
    }
}

impl<'de, 'a> SeqAccess<'de> for &'a mut Deserializer<'de> {
    type Error = Error;

    fn next_element_seed<T>(&mut self, seed: T) -> Result<Option<T::Value>>
    where
        T: DeserializeSeed<'de>,
    {
        if matches!(self.event_source.peek(), Some(Event::EndSequence)) {
            return Ok(None);
        }
        seed.deserialize(&mut **self).map(Some)
    }
}

impl<'de, 'a> MapAccess<'de> for &'a mut Deserializer<'de> {
    type Error = Error;

    fn next_key_seed<K>(&mut self, seed: K) -> Result<Option<K::Value>>
    where
        K: DeserializeSeed<'de>,
    {
        if matches!(self.event_source.peek(), Some(Event::EndMap)) {
            return Ok(None);
        }
        seed.deserialize(&mut **self).map(Some)
    }

    fn next_value_seed<V>(&mut self, seed: V) -> Result<V::Value>
    where
        V: DeserializeSeed<'de>,
    {
        seed.deserialize(&mut **self)
    }
}
