use std::{collections::HashMap, sync::Arc};

use arrow::{
    array::{
        ArrayRef, BooleanBuilder, Date64Builder, Float32Builder, Float64Builder, Int16Builder,
        Int32Builder, Int64Builder, Int8Builder, LargeStringBuilder, StringBuilder, UInt16Builder,
        UInt32Builder, UInt64Builder, UInt8Builder,
    },
    datatypes::{DataType as ArrowType, Schema as ArrowSchema},
    record_batch::RecordBatch,
};
use chrono::{DateTime, NaiveDateTime, Utc};
use serde::Serialize;

use crate::{
    error,
    event::{serialize_into_sink, Event, EventSink},
    fail, DataType, Result, Schema,
};

const DEFAULT_CAPACITY: usize = 1024;

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

pub enum ArrayBuilder {
    Bool(BooleanBuilder),
    I8(Int8Builder),
    I16(Int16Builder),
    I32(Int32Builder),
    I64(Int64Builder),
    U8(UInt8Builder),
    U16(UInt16Builder),
    U32(UInt32Builder),
    U64(UInt64Builder),
    F32(Float32Builder),
    F64(Float64Builder),
    Utf8(StringBuilder),
    LargeUtf8(LargeStringBuilder),
    Date64(Date64Builder),
    Date64Str(Date64Builder),
    Date64NaiveStr(Date64Builder),
}

macro_rules! dispatch {
    ($obj:ident, $builder:pat => $expr:expr) => {
        match $obj {
            ArrayBuilder::Bool($builder) => $expr,
            ArrayBuilder::I8($builder) => $expr,
            ArrayBuilder::I16($builder) => $expr,
            ArrayBuilder::I32($builder) => $expr,
            ArrayBuilder::I64($builder) => $expr,
            ArrayBuilder::U8($builder) => $expr,
            ArrayBuilder::U16($builder) => $expr,
            ArrayBuilder::U32($builder) => $expr,
            ArrayBuilder::U64($builder) => $expr,
            ArrayBuilder::F32($builder) => $expr,
            ArrayBuilder::F64($builder) => $expr,
            ArrayBuilder::Utf8($builder) => $expr,
            ArrayBuilder::LargeUtf8($builder) => $expr,
            ArrayBuilder::Date64($builder) => $expr,
            ArrayBuilder::Date64Str($builder) => $expr,
            ArrayBuilder::Date64NaiveStr($builder) => $expr,
        }
    };
}

impl ArrayBuilder {
    pub fn new(data_type: &DataType) -> Result<Self> {
        let res = match data_type {
            DataType::Bool | DataType::Arrow(ArrowType::Boolean) => {
                Self::Bool(BooleanBuilder::new(DEFAULT_CAPACITY))
            }
            DataType::I8 | DataType::Arrow(ArrowType::Int8) => {
                Self::I8(Int8Builder::new(DEFAULT_CAPACITY))
            }
            DataType::I16 | DataType::Arrow(ArrowType::Int16) => {
                Self::I16(Int16Builder::new(DEFAULT_CAPACITY))
            }
            DataType::I32 | DataType::Arrow(ArrowType::Int32) => {
                Self::I32(Int32Builder::new(DEFAULT_CAPACITY))
            }
            DataType::I64 | DataType::Arrow(ArrowType::Int64) => {
                Self::I64(Int64Builder::new(DEFAULT_CAPACITY))
            }
            DataType::U8 | DataType::Arrow(ArrowType::UInt8) => {
                Self::U8(UInt8Builder::new(DEFAULT_CAPACITY))
            }
            DataType::U16 | DataType::Arrow(ArrowType::UInt16) => {
                Self::U16(UInt16Builder::new(DEFAULT_CAPACITY))
            }
            DataType::U32 | DataType::Arrow(ArrowType::UInt32) => {
                Self::U32(UInt32Builder::new(DEFAULT_CAPACITY))
            }
            DataType::U64 | DataType::Arrow(ArrowType::UInt64) => {
                Self::U64(UInt64Builder::new(DEFAULT_CAPACITY))
            }
            DataType::F32 | DataType::Arrow(ArrowType::Float32) => {
                Self::F32(Float32Builder::new(DEFAULT_CAPACITY))
            }
            DataType::F64 | DataType::Arrow(ArrowType::Float64) => {
                Self::F64(Float64Builder::new(DEFAULT_CAPACITY))
            }
            DataType::Str | DataType::Arrow(ArrowType::Utf8) => {
                Self::Utf8(StringBuilder::new(DEFAULT_CAPACITY))
            }
            DataType::Arrow(ArrowType::LargeUtf8) => {
                Self::LargeUtf8(LargeStringBuilder::new(DEFAULT_CAPACITY))
            }
            DataType::DateTimeMilliseconds | DataType::Arrow(ArrowType::Date64) => {
                Self::Date64(Date64Builder::new(DEFAULT_CAPACITY))
            }
            DataType::NaiveDateTimeStr => {
                Self::Date64NaiveStr(Date64Builder::new(DEFAULT_CAPACITY))
            }
            DataType::DateTimeStr => Self::Date64Str(Date64Builder::new(DEFAULT_CAPACITY)),
            _ => fail!("Cannot build ArrayBuilder for {:?}", data_type),
        };
        Ok(res)
    }

    pub fn build(&mut self) -> Result<ArrayRef> {
        let array_ref: ArrayRef = dispatch!(self, builder => Arc::new(builder.finish()));
        Ok(array_ref)
    }

    pub fn append_null(&mut self) -> Result<()> {
        dispatch!(self, builder => builder.append_null()?);
        Ok(())
    }
}

macro_rules! simple_append {
    ($name:ident, $ty:ty, $variant:ident) => {
        pub fn $name(&mut self, value: $ty) -> Result<()> {
            match self {
                Self::$variant(builder) => builder.append_value(value)?,
                _ => fail!("Mismatched type: cannot insert {}", stringify!($ty)),
            };
            Ok(())
        }
    };
}

impl ArrayBuilder {
    simple_append!(append_bool, bool, Bool);
    simple_append!(append_i8, i8, I8);
    simple_append!(append_i16, i16, I16);
    simple_append!(append_i32, i32, I32);
    simple_append!(append_u8, u8, U8);
    simple_append!(append_u16, u16, U16);
    simple_append!(append_u32, u32, U32);
    simple_append!(append_u64, u64, U64);
    simple_append!(append_f32, f32, F32);
    simple_append!(append_f64, f64, F64);

    pub fn append_i64(&mut self, value: i64) -> Result<()> {
        match self {
            Self::I64(builder) => builder.append_value(value)?,
            Self::Date64(builder) => builder.append_value(value)?,
            _ => fail!("Mismatched type: cannot insert {}", stringify!($ty)),
        };
        Ok(())
    }

    pub fn append_utf8(&mut self, data: &str) -> Result<()> {
        match self {
            Self::Utf8(builder) => builder.append_value(data)?,
            Self::LargeUtf8(builder) => builder.append_value(data)?,
            Self::Date64NaiveStr(builder) => {
                let dt = data.parse::<NaiveDateTime>()?;
                builder.append_value(dt.timestamp_millis())?;
            }
            Self::Date64Str(builder) => {
                let dt = data.parse::<DateTime<Utc>>()?;
                builder.append_value(dt.timestamp_millis())?;
            }
            _ => fail!("Mismatched type"),
        };
        Ok(())
    }
}
