use crate::ops::{from_arrays, ArraySource};
use crate::{event::Event, fail, DataType, Result, Schema};

use arrow2::{
    array::{
        Array,
        BooleanArray,
        // Date64Array,
        Float32Array,
        Float64Array,
        Int16Array,
        Int32Array,
        Int64Array,
        Int8Array,
        UInt16Array,
        UInt32Array,
        UInt64Array,
        UInt8Array,
        Utf8Array,
    },
    chunk::Chunk,
    datatypes::DataType as Arrow2DataType,
};
use chrono::{NaiveDateTime, TimeZone, Utc};

use serde::Deserialize;

pub fn from_chunk<'de, T: Deserialize<'de>, A: AsRef<dyn Array>>(
    chunk: &'de Chunk<A>,
    schema: &Schema,
) -> Result<T> {
    let arrays = build_arrays(chunk, schema)?;
    from_arrays(arrays, chunk.len())
}

struct Arrow2ArraySource<'a> {
    name: String,
    array: Arrow2ArrayRef<'a>,
}

enum Arrow2ArrayRef<'a> {
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
    Utf8I32(&'a Utf8Array<i32>),
    Utf8I64(&'a Utf8Array<i64>),
    Date64NaiveDateTimeStr(&'a Int64Array),
    Date64DateTimeStr(&'a Int64Array),
    Date64DateTimeMilliseconds(&'a Int64Array),
}

impl<'a> ArraySource for Arrow2ArraySource<'a> {
    fn name(&self) -> &str {
        &self.name
    }

    fn emit<'this, 'event>(&'this self, idx: usize) -> Event<'event> {
        macro_rules! emit {
            ($arr:expr, $idx:expr) => {
                if $arr.is_null($idx) {
                    Event::Null
                } else {
                    $arr.value($idx).to_owned().into()
                }
            };
        }

        use Arrow2ArrayRef::*;
        match self.array {
            Bool(arr) => emit!(arr, idx),
            I8(arr) => emit!(arr, idx),
            I16(arr) => emit!(arr, idx),
            I32(arr) => emit!(arr, idx),
            I64(arr) => emit!(arr, idx),
            U8(arr) => emit!(arr, idx),
            U16(arr) => emit!(arr, idx),
            U32(arr) => emit!(arr, idx),
            U64(arr) => emit!(arr, idx),
            F32(arr) => emit!(arr, idx),
            F64(arr) => emit!(arr, idx),
            Utf8I32(arr) => emit!(arr, idx),
            Utf8I64(arr) => emit!(arr, idx),
            Date64DateTimeMilliseconds(arr) => emit!(arr, idx),
            Date64NaiveDateTimeStr(arr) => {
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
            Date64DateTimeStr(arr) => {
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

fn build_arrays<'a, A: AsRef<dyn Array>>(
    chunk: &'a Chunk<A>,
    schema: &Schema,
) -> Result<Vec<Arrow2ArraySource<'a>>> {
    if schema.fields().len() != chunk.arrays().len() {
        fail!("Chunk and schema have a different number of fields");
    }

    let mut arrays = Vec::new();

    for (i, name) in schema.fields().iter().enumerate() {
        let name = name.to_owned();
        let col = chunk.arrays()[i].as_ref();
        let data_type = schema.data_type(&name);

        let array = match col.data_type() {
            Arrow2DataType::Boolean => Arrow2ArrayRef::Bool(col.as_any().downcast_ref().unwrap()),
            Arrow2DataType::Int8 => Arrow2ArrayRef::I8(col.as_any().downcast_ref().unwrap()),
            Arrow2DataType::Int16 => Arrow2ArrayRef::I16(col.as_any().downcast_ref().unwrap()),
            Arrow2DataType::Int32 => Arrow2ArrayRef::I32(col.as_any().downcast_ref().unwrap()),
            Arrow2DataType::Int64 => Arrow2ArrayRef::I64(col.as_any().downcast_ref().unwrap()),
            Arrow2DataType::UInt8 => Arrow2ArrayRef::U8(col.as_any().downcast_ref().unwrap()),
            Arrow2DataType::UInt16 => Arrow2ArrayRef::U16(col.as_any().downcast_ref().unwrap()),
            Arrow2DataType::UInt32 => Arrow2ArrayRef::U32(col.as_any().downcast_ref().unwrap()),
            Arrow2DataType::UInt64 => Arrow2ArrayRef::U64(col.as_any().downcast_ref().unwrap()),
            Arrow2DataType::Float32 => Arrow2ArrayRef::F32(col.as_any().downcast_ref().unwrap()),
            Arrow2DataType::Float64 => Arrow2ArrayRef::F64(col.as_any().downcast_ref().unwrap()),
            Arrow2DataType::Utf8 => Arrow2ArrayRef::Utf8I32(col.as_any().downcast_ref().unwrap()),
            Arrow2DataType::LargeUtf8 => {
                Arrow2ArrayRef::Utf8I64(col.as_any().downcast_ref().unwrap())
            }
            Arrow2DataType::Date32 => fail!("Date32 are not supported at the moment"),
            Arrow2DataType::Date64 => match data_type {
                Some(DataType::DateTimeMilliseconds) => {
                    Arrow2ArrayRef::Date64DateTimeMilliseconds(col.as_any().downcast_ref().unwrap())
                }
                Some(DataType::NaiveDateTimeStr) => {
                    Arrow2ArrayRef::Date64NaiveDateTimeStr(col.as_any().downcast_ref().unwrap())
                }
                Some(DataType::DateTimeStr) => {
                    Arrow2ArrayRef::Date64DateTimeStr(col.as_any().downcast_ref().unwrap())
                }
                Some(dt) => fail!("Annotation {} is not supported by Date64", dt),
                None => fail!("Date64 columns require additional data type annotations"),
            },
            dt => fail!("Arrow DataType {:?} not understood", dt),
        };
        arrays.push(Arrow2ArraySource { name, array });
    }

    Ok(arrays)
}
