use std::sync::Arc;

use marrow::{
    array::{
        Array, BooleanArray, BytesArray, DictionaryArray, FixedSizeListArray, ListArray, MapArray,
        NullArray, PrimitiveArray, TimeArray, TimestampArray,
    },
    datatypes::{FieldMeta, MapMeta, TimeUnit},
    view::{BitsWithOffset, PrimitiveView, View},
};

use super::utils::{as_array_ref, assert_arrays_eq, view_as, PanicOnError};

#[test]
fn slicing() -> PanicOnError<()> {
    let array_via_arrow =
        as_array_ref::<arrow_array::Int64Array>(vec![Some(1), Some(-2), None, None]);

    assert_eq!(
        view_as!(View::Int64, array_via_arrow)?,
        PrimitiveView {
            validity: Some(BitsWithOffset {
                offset: 0,
                data: &marrow::bit_array![true, true, false, false],
            }),
            values: &[1, -2, 0, 0],
        },
    );

    let slice_ref = array_via_arrow.slice(1, 3);
    assert_eq!(
        view_as!(View::Int64, slice_ref)?,
        PrimitiveView {
            validity: Some(BitsWithOffset {
                offset: 1,
                data: &marrow::bit_array![true, true, false, false]
            }),
            values: &[-2, 0, 0],
        },
    );

    let slice_ref = array_via_arrow.slice(2, 2);
    assert_eq!(
        view_as!(View::Int64, slice_ref)?,
        PrimitiveView {
            validity: Some(BitsWithOffset {
                offset: 2,
                data: &marrow::bit_array![true, true, false, false]
            }),
            values: &[0, 0],
        },
    );

    let slice_ref = array_via_arrow.slice(3, 1);
    assert_eq!(
        view_as!(View::Int64, slice_ref)?,
        PrimitiveView {
            validity: Some(BitsWithOffset {
                offset: 3,
                data: &marrow::bit_array![true, true, false, false]
            }),
            values: &[0],
        },
    );

    let slice_ref = array_via_arrow.slice(4, 0);
    assert_eq!(
        view_as!(View::Int64, slice_ref)?,
        PrimitiveView {
            validity: Some(BitsWithOffset {
                offset: 4,
                data: &marrow::bit_array![true, true, false, false]
            }),
            values: &[],
        },
    );

    Ok(())
}

mod null {
    use super::*;

    use arrow_array::ArrayRef;

    #[test]
    fn example() -> PanicOnError<()> {
        assert_arrays_eq(
            Arc::new(arrow_array::NullArray::new(3)) as ArrayRef,
            Array::Null(NullArray { len: 3 }),
        )
    }
}

mod boolean {
    use super::*;

    #[test]
    fn non_nullable() -> PanicOnError<()> {
        assert_arrays_eq(
            as_array_ref::<arrow_array::BooleanArray>(vec![true, true, false, false, false]),
            Array::Boolean(BooleanArray {
                len: 5,
                validity: None,
                values: marrow::bit_vec![true, true, false, false, false],
            }),
        )
    }

    #[test]
    fn nullable() -> PanicOnError<()> {
        assert_arrays_eq(
            as_array_ref::<arrow_array::BooleanArray>(vec![
                Some(true),
                None,
                None,
                Some(false),
                Some(false),
            ]),
            Array::Boolean(BooleanArray {
                len: 5,
                validity: Some(marrow::bit_vec![true, false, false, true, true]),
                values: marrow::bit_vec![true, false, false, false, false],
            }),
        )
    }
}

mod int8 {
    use super::*;

    #[test]
    fn not_nullable() -> PanicOnError<()> {
        assert_arrays_eq(
            as_array_ref::<arrow_array::Int8Array>(vec![1, -2, 3, -4]),
            Array::Int8(PrimitiveArray {
                validity: None,
                values: vec![1, -2, 3, -4],
            }),
        )
    }

    #[test]
    fn nullable() -> PanicOnError<()> {
        assert_arrays_eq(
            as_array_ref::<arrow_array::Int8Array>(vec![Some(1), Some(-2), None, None]),
            Array::Int8(PrimitiveArray {
                validity: Some(marrow::bit_vec![true, true, false, false]),
                values: vec![1, -2, 0, 0],
            }),
        )
    }
}

mod int16 {
    use super::*;

    #[test]
    fn not_nullable() -> PanicOnError<()> {
        assert_arrays_eq(
            as_array_ref::<arrow_array::Int16Array>(vec![1, -2, 3, -4]),
            Array::Int16(PrimitiveArray {
                validity: None,
                values: vec![1, -2, 3, -4],
            }),
        )
    }

    #[test]
    fn nullable() -> PanicOnError<()> {
        assert_arrays_eq(
            as_array_ref::<arrow_array::Int16Array>(vec![Some(1), Some(-2), None, None]),
            Array::Int16(PrimitiveArray {
                validity: Some(marrow::bit_vec![true, true, false, false]),
                values: vec![1, -2, 0, 0],
            }),
        )
    }
}

mod int32 {
    use super::*;

    #[test]
    fn not_nullable() -> PanicOnError<()> {
        assert_arrays_eq(
            as_array_ref::<arrow_array::Int32Array>(vec![1, -2, 3, -4]),
            Array::Int32(PrimitiveArray {
                validity: None,
                values: vec![1, -2, 3, -4],
            }),
        )
    }

    #[test]
    fn nullable() -> PanicOnError<()> {
        assert_arrays_eq(
            as_array_ref::<arrow_array::Int32Array>(vec![Some(1), Some(-2), None, None]),
            Array::Int32(PrimitiveArray {
                validity: Some(marrow::bit_vec![true, true, false, false]),
                values: vec![1, -2, 0, 0],
            }),
        )
    }
}

mod int64 {
    use super::*;

    #[test]
    fn not_nullable() -> PanicOnError<()> {
        assert_arrays_eq(
            as_array_ref::<arrow_array::Int64Array>(vec![1, -2, 3, -4]),
            Array::Int64(PrimitiveArray {
                validity: None,
                values: vec![1, -2, 3, -4],
            }),
        )
    }

    #[test]
    fn nullable() -> PanicOnError<()> {
        assert_arrays_eq(
            as_array_ref::<arrow_array::Int64Array>(vec![Some(1), Some(-2), None, None]),
            Array::Int64(PrimitiveArray {
                validity: Some(marrow::bit_vec![true, true, false, false]),
                values: vec![1, -2, 0, 0],
            }),
        )
    }
}

mod uint8 {
    use super::*;

    #[test]
    fn not_nullable() -> PanicOnError<()> {
        assert_arrays_eq(
            as_array_ref::<arrow_array::UInt8Array>(vec![1, 2, 3, 4]),
            Array::UInt8(PrimitiveArray {
                validity: None,
                values: vec![1, 2, 3, 4],
            }),
        )
    }

    #[test]
    fn nullable() -> PanicOnError<()> {
        assert_arrays_eq(
            as_array_ref::<arrow_array::UInt8Array>(vec![Some(1), Some(2), None, None]),
            Array::UInt8(PrimitiveArray {
                validity: Some(marrow::bit_vec![true, true, false, false]),
                values: vec![1, 2, 0, 0],
            }),
        )
    }
}

mod uint16 {
    use super::*;

    #[test]
    fn not_nullable() -> PanicOnError<()> {
        assert_arrays_eq(
            as_array_ref::<arrow_array::UInt16Array>(vec![1, 2, 3, 4]),
            Array::UInt16(PrimitiveArray {
                validity: None,
                values: vec![1, 2, 3, 4],
            }),
        )
    }

    #[test]
    fn nullable() -> PanicOnError<()> {
        assert_arrays_eq(
            as_array_ref::<arrow_array::UInt16Array>(vec![Some(1), Some(2), None, None]),
            Array::UInt16(PrimitiveArray {
                validity: Some(marrow::bit_vec![true, true, false, false]),
                values: vec![1, 2, 0, 0],
            }),
        )
    }
}

mod uint32 {
    use super::*;

    #[test]
    fn not_nullable() -> PanicOnError<()> {
        assert_arrays_eq(
            as_array_ref::<arrow_array::UInt32Array>(vec![1, 2, 3, 4]),
            Array::UInt32(PrimitiveArray {
                validity: None,
                values: vec![1, 2, 3, 4],
            }),
        )
    }

    #[test]
    fn nullable() -> PanicOnError<()> {
        assert_arrays_eq(
            as_array_ref::<arrow_array::UInt32Array>(vec![Some(1), Some(2), None, None]),
            Array::UInt32(PrimitiveArray {
                validity: Some(marrow::bit_vec![true, true, false, false]),
                values: vec![1, 2, 0, 0],
            }),
        )
    }
}

mod uint64 {
    use super::*;

    #[test]
    fn not_nullable() -> PanicOnError<()> {
        assert_arrays_eq(
            as_array_ref::<arrow_array::UInt64Array>(vec![1, 2, 3, 4]),
            Array::UInt64(PrimitiveArray {
                validity: None,
                values: vec![1, 2, 3, 4],
            }),
        )
    }

    #[test]
    fn nullable() -> PanicOnError<()> {
        assert_arrays_eq(
            as_array_ref::<arrow_array::UInt64Array>(vec![Some(1), Some(2), None, None]),
            Array::UInt64(PrimitiveArray {
                validity: Some(marrow::bit_vec![true, true, false, false]),
                values: vec![1, 2, 0, 0],
            }),
        )
    }
}

mod float16 {
    use super::*;

    use half::f16;

    #[test]
    fn not_nullable() -> PanicOnError<()> {
        assert_arrays_eq(
            as_array_ref::<arrow_array::Float16Array>(vec![
                f16::from_f64(13.0),
                f16::from_f64(21.0),
                f16::from_f64(42.0),
            ]),
            Array::Float16(PrimitiveArray {
                validity: None,
                values: vec![
                    f16::from_f64(13.0),
                    f16::from_f64(21.0),
                    f16::from_f64(42.0),
                ],
            }),
        )
    }

    #[test]
    fn nullable() -> PanicOnError<()> {
        assert_arrays_eq(
            as_array_ref::<arrow_array::Float16Array>(vec![None, None, Some(f16::from_f64(42.0))]),
            Array::Float16(PrimitiveArray {
                validity: Some(marrow::bit_vec![false, false, true]),
                values: vec![f16::from_f64(0.0), f16::from_f64(0.0), f16::from_f64(42.0)],
            }),
        )
    }
}

mod float32 {
    use super::*;

    #[test]
    fn not_nullable() -> PanicOnError<()> {
        assert_arrays_eq(
            as_array_ref::<arrow_array::Float32Array>(vec![13.0, 21.0, 42.0]),
            Array::Float32(PrimitiveArray {
                validity: None,
                values: vec![13.0, 21.0, 42.0],
            }),
        )
    }

    #[test]
    fn nullable() -> PanicOnError<()> {
        assert_arrays_eq(
            as_array_ref::<arrow_array::Float32Array>(vec![None, None, Some(42.0)]),
            Array::Float32(PrimitiveArray {
                validity: Some(marrow::bit_vec![false, false, true]),
                values: vec![0.0, 0.0, 42.0],
            }),
        )
    }
}

mod float64 {
    use super::*;

    #[test]
    fn not_nullable() -> PanicOnError<()> {
        assert_arrays_eq(
            as_array_ref::<arrow_array::Float64Array>(vec![13.0, 21.0, 42.0]),
            Array::Float64(PrimitiveArray {
                validity: None,
                values: vec![13.0, 21.0, 42.0],
            }),
        )
    }

    #[test]
    fn nullable() -> PanicOnError<()> {
        assert_arrays_eq(
            as_array_ref::<arrow_array::Float64Array>(vec![None, None, Some(42.0)]),
            Array::Float64(PrimitiveArray {
                validity: Some(marrow::bit_vec![false, false, true]),
                values: vec![0.0, 0.0, 42.0],
            }),
        )
    }
}

mod date32 {
    use super::*;

    use arrow_array::types::Date32Type;
    use chrono::NaiveDate;

    fn ymd_as_num(y: i32, m: u32, d: u32) -> i32 {
        let unix_epoch = NaiveDate::from_ymd_opt(1970, 1, 1).unwrap();
        (NaiveDate::from_ymd_opt(y, m, d).unwrap() - unix_epoch).num_days() as i32
    }

    #[test]
    fn not_nullable() -> PanicOnError<()> {
        assert_arrays_eq(
            as_array_ref::<arrow_array::Date32Array>(vec![
                Date32Type::from_naive_date(NaiveDate::from_ymd_opt(2024, 10, 8).unwrap()),
                Date32Type::from_naive_date(NaiveDate::from_ymd_opt(-10, 12, 31).unwrap()),
            ]),
            Array::Date32(PrimitiveArray {
                validity: None,
                values: vec![ymd_as_num(2024, 10, 8), ymd_as_num(-10, 12, 31)],
            }),
        )
    }

    #[test]
    fn nullable() -> PanicOnError<()> {
        assert_arrays_eq(
            as_array_ref::<arrow_array::Date32Array>(vec![
                Some(Date32Type::from_naive_date(
                    NaiveDate::from_ymd_opt(2024, 10, 8).unwrap(),
                )),
                None,
                Some(Date32Type::from_naive_date(
                    NaiveDate::from_ymd_opt(-10, 12, 31).unwrap(),
                )),
            ]),
            Array::Date32(PrimitiveArray {
                validity: Some(marrow::bit_vec![true, false, true]),
                values: vec![ymd_as_num(2024, 10, 8), 0, ymd_as_num(-10, 12, 31)],
            }),
        )
    }
}

mod date64 {
    use super::*;

    use arrow_array::types::Date64Type;
    use chrono::NaiveDate;

    fn ymd_as_num(y: i32, m: u32, d: u32) -> i64 {
        let unix_epoch = NaiveDate::from_ymd_opt(1970, 1, 1).unwrap();
        (NaiveDate::from_ymd_opt(y, m, d).unwrap() - unix_epoch).num_days() as i64
            * (24 * 60 * 60 * 1000)
    }

    #[test]
    fn not_nullable() -> PanicOnError<()> {
        assert_arrays_eq(
            as_array_ref::<arrow_array::Date64Array>(vec![
                Date64Type::from_naive_date(NaiveDate::from_ymd_opt(2024, 10, 8).unwrap()),
                Date64Type::from_naive_date(NaiveDate::from_ymd_opt(-10, 12, 31).unwrap()),
            ]),
            Array::Date64(PrimitiveArray {
                validity: None,
                values: vec![ymd_as_num(2024, 10, 8), ymd_as_num(-10, 12, 31)],
            }),
        )
    }

    #[test]
    fn nullable() -> PanicOnError<()> {
        assert_arrays_eq(
            as_array_ref::<arrow_array::Date64Array>(vec![
                Some(Date64Type::from_naive_date(
                    NaiveDate::from_ymd_opt(2024, 10, 8).unwrap(),
                )),
                None,
                Some(Date64Type::from_naive_date(
                    NaiveDate::from_ymd_opt(-10, 12, 31).unwrap(),
                )),
            ]),
            Array::Date64(PrimitiveArray {
                validity: Some(marrow::bit_vec![true, false, true]),
                values: vec![ymd_as_num(2024, 10, 8), 0, ymd_as_num(-10, 12, 31)],
            }),
        )
    }
}

mod time32_seconds {
    use super::*;

    #[test]
    fn not_nullable() -> PanicOnError<()> {
        assert_arrays_eq(
            as_array_ref::<arrow_array::Time32SecondArray>(vec![1, 2, 3]),
            Array::Time32(TimeArray {
                unit: TimeUnit::Second,
                validity: None,
                values: vec![1, 2, 3],
            }),
        )
    }

    #[test]
    fn nullable() -> PanicOnError<()> {
        assert_arrays_eq(
            as_array_ref::<arrow_array::Time32SecondArray>(vec![None, None, Some(3), Some(4)]),
            Array::Time32(TimeArray {
                unit: TimeUnit::Second,
                validity: Some(marrow::bit_vec![false, false, true, true]),
                values: vec![0, 0, 3, 4],
            }),
        )
    }
}

mod time32_milliseconds {
    use super::*;

    #[test]
    fn not_nullable() -> PanicOnError<()> {
        assert_arrays_eq(
            as_array_ref::<arrow_array::Time32MillisecondArray>(vec![1, 2, 3]),
            Array::Time32(TimeArray {
                unit: TimeUnit::Millisecond,
                validity: None,
                values: vec![1, 2, 3],
            }),
        )
    }

    #[test]
    fn nullable() -> PanicOnError<()> {
        assert_arrays_eq(
            as_array_ref::<arrow_array::Time32MillisecondArray>(vec![None, None, Some(3), Some(4)]),
            Array::Time32(TimeArray {
                unit: TimeUnit::Millisecond,
                validity: Some(marrow::bit_vec![false, false, true, true]),
                values: vec![0, 0, 3, 4],
            }),
        )
    }
}

mod time64_microsecond {
    use super::*;

    #[test]
    fn not_nullable() -> PanicOnError<()> {
        assert_arrays_eq(
            as_array_ref::<arrow_array::Time64MicrosecondArray>(vec![1, 2, 3]),
            Array::Time64(TimeArray {
                unit: TimeUnit::Microsecond,
                validity: None,
                values: vec![1, 2, 3],
            }),
        )
    }

    #[test]
    fn nullable() -> PanicOnError<()> {
        assert_arrays_eq(
            as_array_ref::<arrow_array::Time64MicrosecondArray>(vec![None, None, Some(3), Some(4)]),
            Array::Time64(TimeArray {
                unit: TimeUnit::Microsecond,
                validity: Some(marrow::bit_vec![false, false, true, true]),
                values: vec![0, 0, 3, 4],
            }),
        )
    }
}

mod time64_nanosecond {
    use super::*;

    #[test]
    fn not_nullable() -> PanicOnError<()> {
        assert_arrays_eq(
            as_array_ref::<arrow_array::Time64NanosecondArray>(vec![1, 2, 3]),
            Array::Time64(TimeArray {
                unit: TimeUnit::Nanosecond,
                validity: None,
                values: vec![1, 2, 3],
            }),
        )
    }

    #[test]
    fn nullable() -> PanicOnError<()> {
        assert_arrays_eq(
            as_array_ref::<arrow_array::Time64NanosecondArray>(vec![None, None, Some(3), Some(4)]),
            Array::Time64(TimeArray {
                unit: TimeUnit::Nanosecond,
                validity: Some(marrow::bit_vec![false, false, true, true]),
                values: vec![0, 0, 3, 4],
            }),
        )
    }
}

mod duration_second {
    use super::*;

    #[test]
    fn not_nullable() -> PanicOnError<()> {
        assert_arrays_eq(
            as_array_ref::<arrow_array::DurationSecondArray>(vec![1, 2, 3]),
            Array::Duration(TimeArray {
                unit: TimeUnit::Second,
                validity: None,
                values: vec![1, 2, 3],
            }),
        )
    }

    #[test]
    fn nullable() -> PanicOnError<()> {
        assert_arrays_eq(
            as_array_ref::<arrow_array::DurationSecondArray>(vec![None, None, Some(3), Some(4)]),
            Array::Duration(TimeArray {
                unit: TimeUnit::Second,
                validity: Some(marrow::bit_vec![false, false, true, true]),
                values: vec![0, 0, 3, 4],
            }),
        )
    }
}

mod duration_millisecond {
    use super::*;

    #[test]
    fn not_nullable() -> PanicOnError<()> {
        assert_arrays_eq(
            as_array_ref::<arrow_array::DurationMillisecondArray>(vec![1, 2, 3]),
            Array::Duration(TimeArray {
                unit: TimeUnit::Millisecond,
                validity: None,
                values: vec![1, 2, 3],
            }),
        )
    }

    #[test]
    fn nullable() -> PanicOnError<()> {
        assert_arrays_eq(
            as_array_ref::<arrow_array::DurationMillisecondArray>(vec![
                None,
                None,
                Some(3),
                Some(4),
            ]),
            Array::Duration(TimeArray {
                unit: TimeUnit::Millisecond,
                validity: Some(marrow::bit_vec![false, false, true, true]),
                values: vec![0, 0, 3, 4],
            }),
        )
    }
}

mod duration_microsecond {
    use super::*;

    #[test]
    fn not_nullable() -> PanicOnError<()> {
        assert_arrays_eq(
            as_array_ref::<arrow_array::DurationMicrosecondArray>(vec![1, 2, 3]),
            Array::Duration(TimeArray {
                unit: TimeUnit::Microsecond,
                validity: None,
                values: vec![1, 2, 3],
            }),
        )
    }

    #[test]
    fn nullable() -> PanicOnError<()> {
        assert_arrays_eq(
            as_array_ref::<arrow_array::DurationMicrosecondArray>(vec![
                None,
                None,
                Some(3),
                Some(4),
            ]),
            Array::Duration(TimeArray {
                unit: TimeUnit::Microsecond,
                validity: Some(marrow::bit_vec![false, false, true, true]),
                values: vec![0, 0, 3, 4],
            }),
        )
    }
}

mod duration_nanosecond {
    use super::*;

    #[test]
    fn not_nullable() -> PanicOnError<()> {
        assert_arrays_eq(
            as_array_ref::<arrow_array::DurationNanosecondArray>(vec![1, 2, 3]),
            Array::Duration(TimeArray {
                unit: TimeUnit::Nanosecond,
                validity: None,
                values: vec![1, 2, 3],
            }),
        )
    }

    #[test]
    fn nullable() -> PanicOnError<()> {
        assert_arrays_eq(
            as_array_ref::<arrow_array::DurationNanosecondArray>(vec![
                None,
                None,
                Some(3),
                Some(4),
            ]),
            Array::Duration(TimeArray {
                unit: TimeUnit::Nanosecond,
                validity: Some(marrow::bit_vec![false, false, true, true]),
                values: vec![0, 0, 3, 4],
            }),
        )
    }
}

mod interval_year_month {
    use super::*;

    #[test]
    fn not_nullable() -> PanicOnError<()> {
        assert_arrays_eq(
            as_array_ref::<arrow_array::IntervalYearMonthArray>(vec![1, 2, 3]),
            Array::YearMonthInterval(PrimitiveArray {
                validity: None,
                values: vec![1, 2, 3],
            }),
        )
    }

    #[test]
    fn nullable() -> PanicOnError<()> {
        assert_arrays_eq(
            as_array_ref::<arrow_array::IntervalYearMonthArray>(vec![Some(1), None, Some(3)]),
            Array::YearMonthInterval(PrimitiveArray {
                validity: Some(marrow::bit_vec![true, false, true]),
                values: vec![1, 0, 3],
            }),
        )
    }
}

mod timestamp_second {
    use super::*;

    #[test]
    fn not_nullable() -> PanicOnError<()> {
        assert_arrays_eq(
            as_array_ref::<arrow_array::TimestampSecondArray>(vec![1, 2, 3]),
            Array::Timestamp(TimestampArray {
                unit: TimeUnit::Second,
                timezone: None,
                validity: None,
                values: vec![1, 2, 3],
            }),
        )
    }

    #[test]
    fn nullable() -> PanicOnError<()> {
        assert_arrays_eq(
            as_array_ref::<arrow_array::TimestampSecondArray>(vec![None, None, Some(3), Some(4)]),
            Array::Timestamp(TimestampArray {
                unit: TimeUnit::Second,
                timezone: None,
                validity: Some(marrow::bit_vec![false, false, true, true]),
                values: vec![0, 0, 3, 4],
            }),
        )
    }
}

mod timestamp_second_utc {
    use super::*;

    #[test]
    fn not_nullable() -> PanicOnError<()> {
        assert_arrays_eq(
            Arc::new(arrow_array::TimestampSecondArray::from(vec![1, 2, 3]).with_timezone("UTC"))
                as arrow_array::ArrayRef,
            Array::Timestamp(TimestampArray {
                unit: TimeUnit::Second,
                timezone: Some(String::from("UTC")),
                validity: None,
                values: vec![1, 2, 3],
            }),
        )
    }

    #[test]
    fn nullable() -> PanicOnError<()> {
        assert_arrays_eq(
            Arc::new(
                arrow_array::TimestampSecondArray::from(vec![None, None, Some(3), Some(4)])
                    .with_timezone("UTC"),
            ) as arrow_array::ArrayRef,
            Array::Timestamp(TimestampArray {
                unit: TimeUnit::Second,
                timezone: Some(String::from("UTC")),
                validity: Some(marrow::bit_vec![false, false, true, true]),
                values: vec![0, 0, 3, 4],
            }),
        )
    }
}

mod timestamp_millisecond {
    use super::*;

    #[test]
    fn not_nullable() -> PanicOnError<()> {
        assert_arrays_eq(
            as_array_ref::<arrow_array::TimestampMillisecondArray>(vec![1, 2, 3]),
            Array::Timestamp(TimestampArray {
                unit: TimeUnit::Millisecond,
                timezone: None,
                validity: None,
                values: vec![1, 2, 3],
            }),
        )
    }

    #[test]
    fn nullable() -> PanicOnError<()> {
        assert_arrays_eq(
            as_array_ref::<arrow_array::TimestampMillisecondArray>(vec![
                None,
                None,
                Some(3),
                Some(4),
            ]),
            Array::Timestamp(TimestampArray {
                unit: TimeUnit::Millisecond,
                timezone: None,
                validity: Some(marrow::bit_vec![false, false, true, true]),
                values: vec![0, 0, 3, 4],
            }),
        )
    }
}

mod timestamp_millisecond_utc {
    use super::*;

    #[test]
    fn not_nullable() -> PanicOnError<()> {
        assert_arrays_eq(
            Arc::new(
                arrow_array::TimestampMillisecondArray::from(vec![1, 2, 3]).with_timezone("UTC"),
            ) as arrow_array::ArrayRef,
            Array::Timestamp(TimestampArray {
                unit: TimeUnit::Millisecond,
                timezone: Some(String::from("UTC")),
                validity: None,
                values: vec![1, 2, 3],
            }),
        )
    }

    #[test]
    fn nullable() -> PanicOnError<()> {
        assert_arrays_eq(
            Arc::new(
                arrow_array::TimestampMillisecondArray::from(vec![None, None, Some(3), Some(4)])
                    .with_timezone("UTC"),
            ) as arrow_array::ArrayRef,
            Array::Timestamp(TimestampArray {
                unit: TimeUnit::Millisecond,
                timezone: Some(String::from("UTC")),
                validity: Some(marrow::bit_vec![false, false, true, true]),
                values: vec![0, 0, 3, 4],
            }),
        )
    }
}

mod timestamp_microsecond {
    use super::*;

    #[test]
    fn not_nullable() -> PanicOnError<()> {
        assert_arrays_eq(
            as_array_ref::<arrow_array::TimestampMicrosecondArray>(vec![1, 2, 3]),
            Array::Timestamp(TimestampArray {
                unit: TimeUnit::Microsecond,
                timezone: None,
                validity: None,
                values: vec![1, 2, 3],
            }),
        )
    }

    #[test]
    fn nullable() -> PanicOnError<()> {
        assert_arrays_eq(
            as_array_ref::<arrow_array::TimestampMicrosecondArray>(vec![
                None,
                None,
                Some(3),
                Some(4),
            ]),
            Array::Timestamp(TimestampArray {
                unit: TimeUnit::Microsecond,
                timezone: None,
                validity: Some(marrow::bit_vec![false, false, true, true]),
                values: vec![0, 0, 3, 4],
            }),
        )
    }
}

mod timestamp_microsecond_utc {
    use super::*;

    #[test]
    fn not_nullable() -> PanicOnError<()> {
        assert_arrays_eq(
            Arc::new(
                arrow_array::TimestampMicrosecondArray::from(vec![1, 2, 3]).with_timezone("UTC"),
            ) as arrow_array::ArrayRef,
            Array::Timestamp(TimestampArray {
                unit: TimeUnit::Microsecond,
                timezone: Some(String::from("UTC")),
                validity: None,
                values: vec![1, 2, 3],
            }),
        )
    }

    #[test]
    fn nullable() -> PanicOnError<()> {
        assert_arrays_eq(
            Arc::new(
                arrow_array::TimestampMicrosecondArray::from(vec![None, None, Some(3), Some(4)])
                    .with_timezone("UTC"),
            ) as arrow_array::ArrayRef,
            Array::Timestamp(TimestampArray {
                unit: TimeUnit::Microsecond,
                timezone: Some(String::from("UTC")),
                validity: Some(marrow::bit_vec![false, false, true, true]),
                values: vec![0, 0, 3, 4],
            }),
        )
    }
}

mod timestamp_nanosecond {
    use super::*;

    #[test]
    fn not_nullable() -> PanicOnError<()> {
        assert_arrays_eq(
            as_array_ref::<arrow_array::TimestampNanosecondArray>(vec![1, 2, 3]),
            Array::Timestamp(TimestampArray {
                unit: TimeUnit::Nanosecond,
                timezone: None,
                validity: None,
                values: vec![1, 2, 3],
            }),
        )
    }

    #[test]
    fn nullable() -> PanicOnError<()> {
        assert_arrays_eq(
            as_array_ref::<arrow_array::TimestampNanosecondArray>(vec![
                None,
                None,
                Some(3),
                Some(4),
            ]),
            Array::Timestamp(TimestampArray {
                unit: TimeUnit::Nanosecond,
                timezone: None,
                validity: Some(marrow::bit_vec![false, false, true, true]),
                values: vec![0, 0, 3, 4],
            }),
        )
    }
}

mod timestamp_nanosecond_utc {
    use super::*;

    #[test]
    fn not_nullable() -> PanicOnError<()> {
        assert_arrays_eq(
            Arc::new(
                arrow_array::TimestampNanosecondArray::from(vec![1, 2, 3]).with_timezone("UTC"),
            ) as arrow_array::ArrayRef,
            Array::Timestamp(TimestampArray {
                unit: TimeUnit::Nanosecond,
                timezone: Some(String::from("UTC")),
                validity: None,
                values: vec![1, 2, 3],
            }),
        )
    }

    #[test]
    fn nullable() -> PanicOnError<()> {
        assert_arrays_eq(
            Arc::new(
                arrow_array::TimestampNanosecondArray::from(vec![None, None, Some(3), Some(4)])
                    .with_timezone("UTC"),
            ) as arrow_array::ArrayRef,
            Array::Timestamp(TimestampArray {
                unit: TimeUnit::Nanosecond,
                timezone: Some(String::from("UTC")),
                validity: Some(marrow::bit_vec![false, false, true, true]),
                values: vec![0, 0, 3, 4],
            }),
        )
    }
}

mod utf8 {
    use super::*;

    #[test]
    fn not_nullable() -> PanicOnError<()> {
        assert_arrays_eq(
            as_array_ref::<arrow_array::StringArray>(vec!["foo", "bar", "baz", "hello", "world"]),
            Array::Utf8(BytesArray {
                validity: None,
                offsets: vec![0, 3, 6, 9, 14, 19],
                data: b"foobarbazhelloworld".to_vec(),
            }),
        )
    }

    #[test]
    fn nullable() -> PanicOnError<()> {
        assert_arrays_eq(
            as_array_ref::<arrow_array::StringArray>(vec![
                Some("foo"),
                Some("bar"),
                None,
                None,
                Some("world"),
            ]),
            Array::Utf8(BytesArray {
                validity: Some(marrow::bit_vec![true, true, false, false, true]),
                offsets: vec![0, 3, 6, 6, 6, 11],
                data: b"foobarworld".to_vec(),
            }),
        )
    }
}

mod large_utf8 {
    use super::*;

    #[test]
    fn not_nullable() -> PanicOnError<()> {
        assert_arrays_eq(
            as_array_ref::<arrow_array::LargeStringArray>(vec![
                "foo", "bar", "baz", "hello", "world",
            ]),
            Array::LargeUtf8(BytesArray {
                validity: None,
                offsets: vec![0, 3, 6, 9, 14, 19],
                data: b"foobarbazhelloworld".to_vec(),
            }),
        )
    }

    #[test]
    fn nullable() -> PanicOnError<()> {
        assert_arrays_eq(
            as_array_ref::<arrow_array::LargeStringArray>(vec![
                Some("foo"),
                Some("bar"),
                None,
                None,
                Some("world"),
            ]),
            Array::LargeUtf8(BytesArray {
                validity: Some(marrow::bit_vec![true, true, false, false, true]),
                offsets: vec![0, 3, 6, 6, 6, 11],
                data: b"foobarworld".to_vec(),
            }),
        )
    }
}

mod binary {
    use super::*;

    #[test]
    fn not_nullable() -> PanicOnError<()> {
        assert_arrays_eq(
            as_array_ref::<arrow_array::BinaryArray>(vec![
                b"foo" as &[u8],
                b"bar",
                b"baz",
                b"hello",
                b"world",
            ]),
            Array::Binary(BytesArray {
                validity: None,
                offsets: vec![0, 3, 6, 9, 14, 19],
                data: b"foobarbazhelloworld".to_vec(),
            }),
        )
    }

    #[test]
    fn nullable() -> PanicOnError<()> {
        assert_arrays_eq(
            as_array_ref::<arrow_array::BinaryArray>(vec![
                Some(b"foo" as &[u8]),
                Some(b"bar"),
                None,
                None,
                Some(b"world"),
            ]),
            Array::Binary(BytesArray {
                validity: Some(marrow::bit_vec![true, true, false, false, true]),
                offsets: vec![0, 3, 6, 6, 6, 11],
                data: b"foobarworld".to_vec(),
            }),
        )
    }
}

mod large_binary {
    use super::*;

    #[test]
    fn not_nullable() -> PanicOnError<()> {
        assert_arrays_eq(
            as_array_ref::<arrow_array::LargeBinaryArray>(vec![
                b"foo" as &[u8],
                b"bar",
                b"baz",
                b"hello",
                b"world",
            ]),
            Array::LargeBinary(BytesArray {
                validity: None,
                offsets: vec![0, 3, 6, 9, 14, 19],
                data: b"foobarbazhelloworld".to_vec(),
            }),
        )
    }

    #[test]
    fn nullable() -> PanicOnError<()> {
        assert_arrays_eq(
            as_array_ref::<arrow_array::LargeBinaryArray>(vec![
                Some(b"foo" as &[u8]),
                Some(b"bar"),
                None,
                None,
                Some(b"world"),
            ]),
            Array::LargeBinary(BytesArray {
                validity: Some(marrow::bit_vec![true, true, false, false, true]),
                offsets: vec![0, 3, 6, 6, 6, 11],
                data: b"foobarworld".to_vec(),
            }),
        )
    }
}

mod list {
    use super::*;

    use arrow_array::{
        builder::{Int32Builder, ListBuilder},
        ArrayRef,
    };

    // Copied from the arrow docs
    //
    // License: Apache Software License 2.0
    // Source: https://github.com/apache/arrow-rs/blob/065c7b8f94264eeb6a1ca23a92795fc4e0d31d51/arrow-array/src/builder/generic_list_builder.rs#L218
    // License: ../../LICENSE.arrow.txt
    // Notice: ../../NOTICE.arrow.txt
    //
    // Original notice:
    //
    // Licensed to the Apache Software Foundation (ASF) under one
    // or more contributor license agreements.  See the NOTICE file
    // distributed with this work for additional information
    // regarding copyright ownership.  The ASF licenses this file
    // to you under the Apache License, Version 2.0 (the
    // "License"); you may not use this file except in compliance
    // with the License.  You may obtain a copy of the License at
    //
    //   http://www.apache.org/licenses/LICENSE-2.0
    //
    // Unless required by applicable law or agreed to in writing,
    // software distributed under the License is distributed on an
    // "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
    // KIND, either express or implied.  See the License for the
    // specific language governing permissions and limitations
    // under the License.
    fn example() -> ArrayRef {
        let mut builder = ListBuilder::new(Int32Builder::new());

        builder.append_value([Some(1), Some(2), Some(3)]);
        builder.append_null();
        builder.append_value([]);
        builder.append_value([None]);

        Arc::new(builder.finish()) as ArrayRef
    }

    #[test]
    fn non_nullable() -> PanicOnError<()> {
        assert_arrays_eq(
            example(),
            Array::List(ListArray {
                validity: Some(marrow::bit_vec![true, false, true, true]),
                offsets: vec![0, 3, 3, 3, 4],
                meta: FieldMeta {
                    name: String::from("item"),
                    nullable: true,
                    metadata: Default::default(),
                },
                elements: Box::new(Array::Int32(PrimitiveArray {
                    validity: Some(marrow::bit_vec![true, true, true, false]),
                    values: vec![1, 2, 3, 0],
                })),
            }),
        )
    }
}

mod large_list {
    use super::*;

    use arrow_array::{
        builder::{Int32Builder, LargeListBuilder},
        ArrayRef,
    };

    // Copied from the arrow docs
    //
    // License: Apache Software License 2.0
    // Source: https://github.com/apache/arrow-rs/blob/065c7b8f94264eeb6a1ca23a92795fc4e0d31d51/arrow-array/src/builder/generic_list_builder.rs#L218
    // License: ../../LICENSE.arrow.txt
    // Notice: ../../NOTICE.arrow.txt
    //
    // Original notice:
    //
    // Licensed to the Apache Software Foundation (ASF) under one
    // or more contributor license agreements.  See the NOTICE file
    // distributed with this work for additional information
    // regarding copyright ownership.  The ASF licenses this file
    // to you under the Apache License, Version 2.0 (the
    // "License"); you may not use this file except in compliance
    // with the License.  You may obtain a copy of the License at
    //
    //   http://www.apache.org/licenses/LICENSE-2.0
    //
    // Unless required by applicable law or agreed to in writing,
    // software distributed under the License is distributed on an
    // "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
    // KIND, either express or implied.  See the License for the
    // specific language governing permissions and limitations
    // under the License.
    fn example() -> ArrayRef {
        let mut builder = LargeListBuilder::new(Int32Builder::new());

        builder.append_value([Some(1), Some(2), Some(3)]);
        builder.append_null();
        builder.append_value([]);
        builder.append_value([None]);

        Arc::new(builder.finish()) as ArrayRef
    }

    #[test]
    fn non_nullable() -> PanicOnError<()> {
        assert_arrays_eq(
            example(),
            Array::LargeList(ListArray {
                validity: Some(marrow::bit_vec![true, false, true, true]),
                offsets: vec![0, 3, 3, 3, 4],
                meta: FieldMeta {
                    name: String::from("item"),
                    nullable: true,
                    metadata: Default::default(),
                },
                elements: Box::new(Array::Int32(PrimitiveArray {
                    validity: Some(marrow::bit_vec!(true, true, true, false)),
                    values: vec![1, 2, 3, 0],
                })),
            }),
        )
    }
}

mod fixed_size_list {
    use super::*;

    use arrow_array::{
        builder::{FixedSizeListBuilder, Int32Builder},
        ArrayRef,
    };

    // Copied from the arrow docs
    //
    // License: Apache Software License 2.0
    // Source: https://github.com/apache/arrow-rs/blob/065c7b8f94264eeb6a1ca23a92795fc4e0d31d51/arrow-array/src/builder/fixed_size_list_builder.rs#L27
    // License: ../../LICENSE.arrow.txt
    // Notice: ../../NOTICE.arrow.txt
    //
    // Original notice:
    //
    // Licensed to the Apache Software Foundation (ASF) under one
    // or more contributor license agreements.  See the NOTICE file
    // distributed with this work for additional information
    // regarding copyright ownership.  The ASF licenses this file
    // to you under the Apache License, Version 2.0 (the
    // "License"); you may not use this file except in compliance
    // with the License.  You may obtain a copy of the License at
    //
    //   http://www.apache.org/licenses/LICENSE-2.0
    //
    // Unless required by applicable law or agreed to in writing,
    // software distributed under the License is distributed on an
    // "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
    // KIND, either express or implied.  See the License for the
    // specific language governing permissions and limitations
    // under the License.
    fn example() -> ArrayRef {
        let mut builder = FixedSizeListBuilder::new(Int32Builder::new(), 3);

        //  [[0, 1, 2], null, [3, null, 5], [6, 7, null]]
        builder.values().append_value(0);
        builder.values().append_value(1);
        builder.values().append_value(2);
        builder.append(true);
        builder.values().append_null();
        builder.values().append_null();
        builder.values().append_null();
        builder.append(false);
        builder.values().append_value(3);
        builder.values().append_null();
        builder.values().append_value(5);
        builder.append(true);
        builder.values().append_value(6);
        builder.values().append_value(7);
        builder.values().append_null();
        builder.append(true);

        Arc::new(builder.finish()) as ArrayRef
    }

    #[test]
    fn non_nullable() -> PanicOnError<()> {
        assert_arrays_eq(
            example(),
            Array::FixedSizeList(FixedSizeListArray {
                len: 4,
                n: 3,
                validity: Some(marrow::bit_vec![true, false, true, true]),
                meta: FieldMeta {
                    name: String::from("item"),
                    nullable: true,
                    metadata: Default::default(),
                },
                elements: Box::new(Array::Int32(PrimitiveArray {
                    validity: Some(marrow::bit_vec![
                        true, true, true, // [0, 1, 2]
                        false, false, false, // null
                        true, false, true, // [3, null, 5]
                        true, true, false, // [6, 7, null]]
                    ]),
                    values: vec![0, 1, 2, 0, 0, 0, 3, 0, 5, 6, 7, 0],
                })),
            }),
        )
    }
}

mod map {
    use super::*;

    use arrow_array::builder::{Int32Builder, MapBuilder, StringBuilder};

    // Copied from the arrow docs
    //
    // License: Apache Software License 2.0
    // Source: https://github.com/apache/arrow-rs/blob/065c7b8f94264eeb6a1ca23a92795fc4e0d31d51/arrow-array/src/builder/map_builder.rs#L30
    // License: ../../LICENSE.arrow.txt
    // Notice: ../../NOTICE.arrow.txt
    //
    // Original notice:
    //
    // Licensed to the Apache Software Foundation (ASF) under one
    // or more contributor license agreements.  See the NOTICE file
    // distributed with this work for additional information
    // regarding copyright ownership.  The ASF licenses this file
    // to you under the Apache License, Version 2.0 (the
    // "License"); you may not use this file except in compliance
    // with the License.  You may obtain a copy of the License at
    //
    //   http://www.apache.org/licenses/LICENSE-2.0
    //
    // Unless required by applicable law or agreed to in writing,
    // software distributed under the License is distributed on an
    // "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
    // KIND, either express or implied.  See the License for the
    // specific language governing permissions and limitations
    // under the License.
    fn example_array() -> PanicOnError<arrow_array::ArrayRef> {
        let string_builder = StringBuilder::new();
        let int_builder = Int32Builder::new();

        // Construct `[{"joe": 1}, {"blogs": 2, "foo": 4}, {}, null]`
        let mut builder = MapBuilder::new(None, string_builder, int_builder);

        builder.keys().append_value("joe");
        builder.values().append_value(1);
        builder.append(true).unwrap();

        builder.keys().append_value("blogs");
        builder.values().append_value(2);
        builder.keys().append_value("foo");
        builder.values().append_value(4);
        builder.append(true).unwrap();
        builder.append(true).unwrap();
        builder.append(false).unwrap();

        Ok(Arc::new(builder.finish()) as arrow_array::ArrayRef)
    }

    #[test]
    fn example() -> PanicOnError<()> {
        assert_arrays_eq(
            example_array()?,
            Array::Map(MapArray {
                meta: MapMeta::default(),
                validity: Some(marrow::bit_vec![true, true, true, false]),
                offsets: vec![0, 1, 3, 3, 3],
                keys: Box::new(Array::Utf8(BytesArray {
                    validity: None,
                    offsets: vec![0, 3, 8, 11],
                    data: b"joeblogsfoo".into(),
                })),
                values: Box::new(Array::Int32(PrimitiveArray {
                    validity: None,
                    values: vec![1, 2, 4],
                })),
            }),
        )
    }
}

mod dictionary {
    use super::*;

    fn build_dictionary_array<K, I>(items: Vec<I>) -> PanicOnError<arrow_array::ArrayRef>
    where
        K: arrow_array::types::ArrowDictionaryKeyType,
        arrow_array::array::DictionaryArray<K>: FromIterator<I>,
    {
        let array: arrow_array::array::DictionaryArray<K> = items.into_iter().collect();
        Ok(Arc::new(array) as arrow_array::ArrayRef)
    }

    #[test]
    fn int8() -> PanicOnError<()> {
        assert_arrays_eq(
            build_dictionary_array::<arrow_array::types::Int8Type, _>(vec![
                "a", "a", "b", "c", "c",
            ])?,
            Array::Dictionary(DictionaryArray {
                keys: Box::new(Array::Int8(PrimitiveArray {
                    validity: None,
                    values: vec![0, 0, 1, 2, 2],
                })),
                values: Box::new(Array::Utf8(BytesArray {
                    validity: None,
                    offsets: vec![0, 1, 2, 3],
                    data: b"abc".to_vec(),
                })),
            }),
        )
    }

    #[test]
    fn int8_nullable() -> PanicOnError<()> {
        assert_arrays_eq(
            build_dictionary_array::<arrow_array::types::Int8Type, _>(vec![
                Some("a"),
                None,
                None,
                Some("c"),
                Some("c"),
            ])?,
            Array::Dictionary(DictionaryArray {
                keys: Box::new(Array::Int8(PrimitiveArray {
                    validity: Some(marrow::bit_vec![true, false, false, true, true]),
                    values: vec![0, 0, 0, 1, 1],
                })),
                values: Box::new(Array::Utf8(BytesArray {
                    validity: None,
                    offsets: vec![0, 1, 2],
                    data: b"ac".to_vec(),
                })),
            }),
        )
    }

    #[test]
    fn int16() -> PanicOnError<()> {
        assert_arrays_eq(
            build_dictionary_array::<arrow_array::types::Int16Type, _>(vec![
                "a", "a", "b", "c", "c",
            ])?,
            Array::Dictionary(DictionaryArray {
                keys: Box::new(Array::Int16(PrimitiveArray {
                    validity: None,
                    values: vec![0, 0, 1, 2, 2],
                })),
                values: Box::new(Array::Utf8(BytesArray {
                    validity: None,
                    offsets: vec![0, 1, 2, 3],
                    data: b"abc".to_vec(),
                })),
            }),
        )
    }

    #[test]
    fn int16_nullable() -> PanicOnError<()> {
        assert_arrays_eq(
            build_dictionary_array::<arrow_array::types::Int16Type, _>(vec![
                Some("a"),
                None,
                None,
                Some("c"),
                Some("c"),
            ])?,
            Array::Dictionary(DictionaryArray {
                keys: Box::new(Array::Int16(PrimitiveArray {
                    validity: Some(marrow::bit_vec![true, false, false, true, true]),
                    values: vec![0, 0, 0, 1, 1],
                })),
                values: Box::new(Array::Utf8(BytesArray {
                    validity: None,
                    offsets: vec![0, 1, 2],
                    data: b"ac".to_vec(),
                })),
            }),
        )
    }

    #[test]
    fn int32() -> PanicOnError<()> {
        assert_arrays_eq(
            build_dictionary_array::<arrow_array::types::Int32Type, _>(vec![
                "a", "a", "b", "c", "c",
            ])?,
            Array::Dictionary(DictionaryArray {
                keys: Box::new(Array::Int32(PrimitiveArray {
                    validity: None,
                    values: vec![0, 0, 1, 2, 2],
                })),
                values: Box::new(Array::Utf8(BytesArray {
                    validity: None,
                    offsets: vec![0, 1, 2, 3],
                    data: b"abc".to_vec(),
                })),
            }),
        )
    }

    #[test]
    fn int32_nullable() -> PanicOnError<()> {
        assert_arrays_eq(
            build_dictionary_array::<arrow_array::types::Int32Type, _>(vec![
                Some("a"),
                None,
                None,
                Some("c"),
                Some("c"),
            ])?,
            Array::Dictionary(DictionaryArray {
                keys: Box::new(Array::Int32(PrimitiveArray {
                    validity: Some(marrow::bit_vec![true, false, false, true, true]),
                    values: vec![0, 0, 0, 1, 1],
                })),
                values: Box::new(Array::Utf8(BytesArray {
                    validity: None,
                    offsets: vec![0, 1, 2],
                    data: b"ac".to_vec(),
                })),
            }),
        )
    }

    #[test]
    fn int64() -> PanicOnError<()> {
        assert_arrays_eq(
            build_dictionary_array::<arrow_array::types::Int64Type, _>(vec![
                "a", "a", "b", "c", "c",
            ])?,
            Array::Dictionary(DictionaryArray {
                keys: Box::new(Array::Int64(PrimitiveArray {
                    validity: None,
                    values: vec![0, 0, 1, 2, 2],
                })),
                values: Box::new(Array::Utf8(BytesArray {
                    validity: None,
                    offsets: vec![0, 1, 2, 3],
                    data: b"abc".to_vec(),
                })),
            }),
        )
    }

    #[test]
    fn int64_nullable() -> PanicOnError<()> {
        assert_arrays_eq(
            build_dictionary_array::<arrow_array::types::Int64Type, _>(vec![
                Some("a"),
                None,
                None,
                Some("c"),
                Some("c"),
            ])?,
            Array::Dictionary(DictionaryArray {
                keys: Box::new(Array::Int64(PrimitiveArray {
                    validity: Some(marrow::bit_vec![true, false, false, true, true]),
                    values: vec![0, 0, 0, 1, 1],
                })),
                values: Box::new(Array::Utf8(BytesArray {
                    validity: None,
                    offsets: vec![0, 1, 2],
                    data: b"ac".to_vec(),
                })),
            }),
        )
    }

    #[test]
    fn uint8() -> PanicOnError<()> {
        assert_arrays_eq(
            build_dictionary_array::<arrow_array::types::UInt8Type, _>(vec![
                "a", "a", "b", "c", "c",
            ])?,
            Array::Dictionary(DictionaryArray {
                keys: Box::new(Array::UInt8(PrimitiveArray {
                    validity: None,
                    values: vec![0, 0, 1, 2, 2],
                })),
                values: Box::new(Array::Utf8(BytesArray {
                    validity: None,
                    offsets: vec![0, 1, 2, 3],
                    data: b"abc".to_vec(),
                })),
            }),
        )
    }

    #[test]
    fn uint8_nullable() -> PanicOnError<()> {
        assert_arrays_eq(
            build_dictionary_array::<arrow_array::types::UInt8Type, _>(vec![
                Some("a"),
                None,
                None,
                Some("c"),
                Some("c"),
            ])?,
            Array::Dictionary(DictionaryArray {
                keys: Box::new(Array::UInt8(PrimitiveArray {
                    validity: Some(marrow::bit_vec![true, false, false, true, true]),
                    values: vec![0, 0, 0, 1, 1],
                })),
                values: Box::new(Array::Utf8(BytesArray {
                    validity: None,
                    offsets: vec![0, 1, 2],
                    data: b"ac".to_vec(),
                })),
            }),
        )
    }

    #[test]
    fn uint16() -> PanicOnError<()> {
        assert_arrays_eq(
            build_dictionary_array::<arrow_array::types::UInt16Type, _>(vec![
                "a", "a", "b", "c", "c",
            ])?,
            Array::Dictionary(DictionaryArray {
                keys: Box::new(Array::UInt16(PrimitiveArray {
                    validity: None,
                    values: vec![0, 0, 1, 2, 2],
                })),
                values: Box::new(Array::Utf8(BytesArray {
                    validity: None,
                    offsets: vec![0, 1, 2, 3],
                    data: b"abc".to_vec(),
                })),
            }),
        )
    }

    #[test]
    fn uint16_nullable() -> PanicOnError<()> {
        assert_arrays_eq(
            build_dictionary_array::<arrow_array::types::UInt16Type, _>(vec![
                Some("a"),
                None,
                None,
                Some("c"),
                Some("c"),
            ])?,
            Array::Dictionary(DictionaryArray {
                keys: Box::new(Array::UInt16(PrimitiveArray {
                    validity: Some(marrow::bit_vec![true, false, false, true, true]),
                    values: vec![0, 0, 0, 1, 1],
                })),
                values: Box::new(Array::Utf8(BytesArray {
                    validity: None,
                    offsets: vec![0, 1, 2],
                    data: b"ac".to_vec(),
                })),
            }),
        )
    }

    #[test]
    fn uint32() -> PanicOnError<()> {
        assert_arrays_eq(
            build_dictionary_array::<arrow_array::types::UInt32Type, _>(vec![
                "a", "a", "b", "c", "c",
            ])?,
            Array::Dictionary(DictionaryArray {
                keys: Box::new(Array::UInt32(PrimitiveArray {
                    validity: None,
                    values: vec![0, 0, 1, 2, 2],
                })),
                values: Box::new(Array::Utf8(BytesArray {
                    validity: None,
                    offsets: vec![0, 1, 2, 3],
                    data: b"abc".to_vec(),
                })),
            }),
        )
    }

    #[test]
    fn uint32_nullable() -> PanicOnError<()> {
        assert_arrays_eq(
            build_dictionary_array::<arrow_array::types::UInt32Type, _>(vec![
                Some("a"),
                None,
                None,
                Some("c"),
                Some("c"),
            ])?,
            Array::Dictionary(DictionaryArray {
                keys: Box::new(Array::UInt32(PrimitiveArray {
                    validity: Some(marrow::bit_vec![true, false, false, true, true]),
                    values: vec![0, 0, 0, 1, 1],
                })),
                values: Box::new(Array::Utf8(BytesArray {
                    validity: None,
                    offsets: vec![0, 1, 2],
                    data: b"ac".to_vec(),
                })),
            }),
        )
    }

    #[test]
    fn uint64() -> PanicOnError<()> {
        assert_arrays_eq(
            build_dictionary_array::<arrow_array::types::UInt64Type, _>(vec![
                "a", "a", "b", "c", "c",
            ])?,
            Array::Dictionary(DictionaryArray {
                keys: Box::new(Array::UInt64(PrimitiveArray {
                    validity: None,
                    values: vec![0, 0, 1, 2, 2],
                })),
                values: Box::new(Array::Utf8(BytesArray {
                    validity: None,
                    offsets: vec![0, 1, 2, 3],
                    data: b"abc".to_vec(),
                })),
            }),
        )
    }

    #[test]
    fn uint64_nullable() -> PanicOnError<()> {
        assert_arrays_eq(
            build_dictionary_array::<arrow_array::types::UInt64Type, _>(vec![
                Some("a"),
                None,
                None,
                Some("c"),
                Some("c"),
            ])?,
            Array::Dictionary(DictionaryArray {
                keys: Box::new(Array::UInt64(PrimitiveArray {
                    validity: Some(marrow::bit_vec![true, false, false, true, true]),
                    values: vec![0, 0, 0, 1, 1],
                })),
                values: Box::new(Array::Utf8(BytesArray {
                    validity: None,
                    offsets: vec![0, 1, 2],
                    data: b"ac".to_vec(),
                })),
            }),
        )
    }
}

mod run_end_encoded {
    use marrow::{
        array::{BytesArray, RunEndEncodedArray},
        datatypes::RunEndEncodedMeta,
    };

    use super::*;

    #[test]
    fn not_nullable() -> PanicOnError<()> {
        assert_arrays_eq(
            Arc::new(
                arrow_array::RunArray::<arrow_array::types::Int32Type>::from_iter([
                    "hello", "hello", "world", "foo",
                ]),
            ) as arrow_array::ArrayRef,
            Array::RunEndEncoded(RunEndEncodedArray {
                meta: RunEndEncodedMeta::default(),
                run_ends: Box::new(Array::Int32(PrimitiveArray {
                    validity: None,
                    values: vec![2, 3, 4],
                })),
                values: Box::new(Array::Utf8(BytesArray {
                    validity: None,
                    offsets: vec![0, 5, 10, 13],
                    data: b"helloworldfoo".to_vec(),
                })),
            }),
        )
    }

    #[test]
    fn nullable() -> PanicOnError<()> {
        assert_arrays_eq(
            Arc::new(
                arrow_array::RunArray::<arrow_array::types::Int32Type>::from_iter([
                    Some("hello"),
                    Some("hello"),
                    None,
                    None,
                    Some("world"),
                    Some("foo"),
                ]),
            ) as arrow_array::ArrayRef,
            Array::RunEndEncoded(RunEndEncodedArray {
                meta: RunEndEncodedMeta::default(),
                run_ends: Box::new(Array::Int32(PrimitiveArray {
                    validity: None,
                    values: vec![2, 4, 5, 6],
                })),
                values: Box::new(Array::Utf8(BytesArray {
                    validity: Some(marrow::bit_vec![true, false, true, true]),
                    offsets: vec![0, 5, 5, 10, 13],
                    data: b"helloworldfoo".to_vec(),
                })),
            }),
        )
    }
}
