use crate::ops::{from_arrays, ArraySource};
use crate::{event::Event, fail, DataType, Result, Schema};

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
    let arrays = build_arrays(record_batch, schema)?;
    from_arrays(arrays, record_batch.num_rows())
}

struct ArrowArraySource<'a> {
    name: String,
    array: ArrowArrayRef<'a>,
}

enum ArrowArrayRef<'a> {
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

impl<'a> ArraySource for ArrowArraySource<'a> {
    fn name(&self) -> &str {
        &self.name
    }

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

        use ArrowArrayRef::*;
        match &self.array {
            Bool(arr) => {
                if arr.is_null(idx) {
                    Event::Null
                } else {
                    arr.value(idx).into()
                }
            }
            I8(arr) => emit_primitive(arr, idx),
            I16(arr) => emit_primitive(arr, idx),
            I32(arr) => emit_primitive(arr, idx),
            I64(arr) => emit_primitive(arr, idx),
            U8(arr) => emit_primitive(arr, idx),
            U16(arr) => emit_primitive(arr, idx),
            U32(arr) => emit_primitive(arr, idx),
            U64(arr) => emit_primitive(arr, idx),
            F32(arr) => emit_primitive(arr, idx),
            F64(arr) => emit_primitive(arr, idx),
            Utf8(arr) => {
                if arr.is_null(idx) {
                    Event::Null
                } else {
                    // TODO: can this be done zero copy?
                    arr.value(idx).to_owned().into()
                }
            }
            LargeUtf8(arr) => {
                if arr.is_null(idx) {
                    Event::Null
                } else {
                    // TODO: can this be done zero copy?
                    arr.value(idx).to_owned().into()
                }
            }
            Date64DateTimeMilliseconds(arr) => emit_primitive(arr, idx),
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

fn build_arrays<'a>(
    record_batch: &'a RecordBatch,
    schema: &Schema,
) -> Result<Vec<ArrowArraySource<'a>>> {
    let mut arrays = Vec::new();

    for (i, column) in record_batch.schema().fields().iter().enumerate() {
        let column = column.name().to_owned();
        let arrow_schema = record_batch.schema();
        let name = arrow_schema.field(i).name();
        let data_type = schema.data_type(name);
        let col = record_batch.column(i);

        let array = match col.data_type() {
            ArrowDataType::Boolean => ArrowArrayRef::Bool(col.as_any().downcast_ref().unwrap()),
            ArrowDataType::Int8 => ArrowArrayRef::I8(col.as_any().downcast_ref().unwrap()),
            ArrowDataType::Int16 => ArrowArrayRef::I16(col.as_any().downcast_ref().unwrap()),
            ArrowDataType::Int32 => ArrowArrayRef::I32(col.as_any().downcast_ref().unwrap()),
            ArrowDataType::Int64 => ArrowArrayRef::I64(col.as_any().downcast_ref().unwrap()),
            ArrowDataType::UInt8 => ArrowArrayRef::U8(col.as_any().downcast_ref().unwrap()),
            ArrowDataType::UInt16 => ArrowArrayRef::U16(col.as_any().downcast_ref().unwrap()),
            ArrowDataType::UInt32 => ArrowArrayRef::U32(col.as_any().downcast_ref().unwrap()),
            ArrowDataType::UInt64 => ArrowArrayRef::U64(col.as_any().downcast_ref().unwrap()),
            ArrowDataType::Float32 => ArrowArrayRef::F32(col.as_any().downcast_ref().unwrap()),
            ArrowDataType::Float64 => ArrowArrayRef::F64(col.as_any().downcast_ref().unwrap()),
            ArrowDataType::Utf8 => ArrowArrayRef::Utf8(col.as_any().downcast_ref().unwrap()),
            ArrowDataType::LargeUtf8 => {
                ArrowArrayRef::LargeUtf8(col.as_any().downcast_ref().unwrap())
            }
            ArrowDataType::Date32 => fail!("Date32 are not supported at the moment"),
            ArrowDataType::Date64 => match data_type {
                Some(DataType::DateTimeMilliseconds) => {
                    ArrowArrayRef::Date64DateTimeMilliseconds(col.as_any().downcast_ref().unwrap())
                }
                Some(DataType::NaiveDateTimeStr) => {
                    ArrowArrayRef::Date64NaiveDateTimeStr(col.as_any().downcast_ref().unwrap())
                }
                Some(DataType::DateTimeStr) => {
                    ArrowArrayRef::Date64DateTimeStr(col.as_any().downcast_ref().unwrap())
                }
                Some(dt) => fail!("Annotation {} is not supported by Date64", dt),
                None => fail!("Date64 columns require additional data type annotations"),
            },
            dt => fail!("Arrow DataType {} not understood", dt),
        };
        arrays.push(ArrowArraySource {
            name: column,
            array,
        });
    }

    Ok(arrays)
}

/*
TODO: fix this test
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
        let record_batch = crate::arrow::to_record_batch(&original, &schema)?;

        let mut event_source = ArraysSource::new(&record_batch, &schema)?;

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
 */
