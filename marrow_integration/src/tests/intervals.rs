// check the layout of the interval types

#[test]
fn interval_layout_day_time() {
    assert_eq!(
        std::mem::size_of::<arrow_array::types::IntervalDayTime>(),
        std::mem::size_of::<marrow::types::DayTimeInterval>(),
    );
    assert_eq!(
        std::mem::align_of::<arrow_array::types::IntervalDayTime>(),
        std::mem::align_of::<marrow::types::DayTimeInterval>(),
    );
    assert_eq!(
        std::mem::offset_of!(arrow_array::types::IntervalDayTime, days),
        std::mem::offset_of!(marrow::types::DayTimeInterval, days),
    );
    assert_eq!(
        std::mem::offset_of!(arrow_array::types::IntervalDayTime, milliseconds),
        std::mem::offset_of!(marrow::types::DayTimeInterval, milliseconds),
    );
}

#[test]
fn interval_layout_month_day_nano() {
    assert_eq!(
        std::mem::size_of::<arrow_array::types::IntervalMonthDayNano>(),
        std::mem::size_of::<marrow::types::MonthDayNanoInterval>(),
    );
    assert_eq!(
        std::mem::align_of::<arrow_array::types::IntervalMonthDayNano>(),
        std::mem::align_of::<marrow::types::MonthDayNanoInterval>(),
    );
    assert_eq!(
        std::mem::offset_of!(arrow_array::types::IntervalMonthDayNano, months),
        std::mem::offset_of!(marrow::types::MonthDayNanoInterval, months),
    );
    assert_eq!(
        std::mem::offset_of!(arrow_array::types::IntervalMonthDayNano, days),
        std::mem::offset_of!(marrow::types::MonthDayNanoInterval, days),
    );
    assert_eq!(
        std::mem::offset_of!(arrow_array::types::IntervalMonthDayNano, nanoseconds),
        std::mem::offset_of!(marrow::types::MonthDayNanoInterval, nanoseconds),
    );
}

// NOTE: the arrow impl of `make_value` is incorrect before `arrow=52`, only test for `arrow>=52`
mod internval_day_time {
    use super::super::utils::{as_array_ref, assert_arrays_eq, PanicOnError};
    use super::arrow_array;
    use marrow::{
        array::{Array, PrimitiveArray},
        types::DayTimeInterval,
    };

    #[test]
    fn not_nullable() -> PanicOnError<()> {
        assert_arrays_eq(
            as_array_ref::<arrow_array::IntervalDayTimeArray>(vec![
                arrow_array::types::IntervalDayTimeType::make_value(1, 2),
                arrow_array::types::IntervalDayTimeType::make_value(3, 4),
                arrow_array::types::IntervalDayTimeType::make_value(5, 6),
            ]),
            Array::DayTimeInterval(PrimitiveArray {
                validity: None,
                values: vec![
                    DayTimeInterval {
                        days: 1,
                        milliseconds: 2,
                    },
                    DayTimeInterval {
                        days: 3,
                        milliseconds: 4,
                    },
                    DayTimeInterval {
                        days: 5,
                        milliseconds: 6,
                    },
                ],
            }),
        )
    }

    #[test]
    fn nullable() -> PanicOnError<()> {
        assert_arrays_eq(
            as_array_ref::<arrow_array::IntervalDayTimeArray>(vec![
                Some(arrow_array::types::IntervalDayTimeType::make_value(1, 2)),
                None,
                Some(arrow_array::types::IntervalDayTimeType::make_value(5, 6)),
            ]),
            Array::DayTimeInterval(PrimitiveArray {
                validity: Some(marrow::bit_vec![true, false, true]),
                values: vec![
                    DayTimeInterval {
                        days: 1,
                        milliseconds: 2,
                    },
                    DayTimeInterval {
                        days: 0,
                        milliseconds: 0,
                    },
                    DayTimeInterval {
                        days: 5,
                        milliseconds: 6,
                    },
                ],
            }),
        )
    }
}

// NOTE: the arrow impl of `make_value` is incorrect before `arrow=52`, only test for `arrow>=52`
mod interval_month_day_nano {
    use super::super::utils::{as_array_ref, assert_arrays_eq, PanicOnError};
    use super::arrow_array;
    use marrow::{
        array::{Array, PrimitiveArray},
        types::MonthDayNanoInterval,
    };

    #[test]
    fn not_nullable() -> PanicOnError<()> {
        assert_arrays_eq(
            as_array_ref::<arrow_array::IntervalMonthDayNanoArray>(vec![
                arrow_array::types::IntervalMonthDayNanoType::make_value(1, 2, 3),
                arrow_array::types::IntervalMonthDayNanoType::make_value(4, 5, 6),
                arrow_array::types::IntervalMonthDayNanoType::make_value(7, 8, 9),
            ]),
            Array::MonthDayNanoInterval(PrimitiveArray {
                validity: None,
                values: vec![
                    MonthDayNanoInterval {
                        months: 1,
                        days: 2,
                        nanoseconds: 3,
                    },
                    MonthDayNanoInterval {
                        months: 4,
                        days: 5,
                        nanoseconds: 6,
                    },
                    MonthDayNanoInterval {
                        months: 7,
                        days: 8,
                        nanoseconds: 9,
                    },
                ],
            }),
        )
    }

    #[test]
    fn nullable() -> PanicOnError<()> {
        assert_arrays_eq(
            as_array_ref::<arrow_array::IntervalMonthDayNanoArray>(vec![
                Some(arrow_array::types::IntervalMonthDayNanoType::make_value(
                    1, 2, 3,
                )),
                None,
                Some(arrow_array::types::IntervalMonthDayNanoType::make_value(
                    7, 8, 9,
                )),
            ]),
            Array::MonthDayNanoInterval(PrimitiveArray {
                validity: Some(marrow::bit_vec![true, false, true]),
                values: vec![
                    MonthDayNanoInterval {
                        months: 1,
                        days: 2,
                        nanoseconds: 3,
                    },
                    MonthDayNanoInterval {
                        months: 0,
                        days: 0,
                        nanoseconds: 0,
                    },
                    MonthDayNanoInterval {
                        months: 7,
                        days: 8,
                        nanoseconds: 9,
                    },
                ],
            }),
        )
    }
}
