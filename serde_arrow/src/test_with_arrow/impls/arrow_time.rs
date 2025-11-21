use chrono::{NaiveTime, Timelike};
use marrow::{
    array::{Array, TimeArray},
    datatypes::{DataType, Field, TimeUnit},
};
use serde::{Deserialize, Serialize};

use crate::{utils::Item, ArrayBuilder, Deserializer, Serializer};

mod time_string_conversion {
    use super::*;

    #[test]
    fn second() {
        assert_times_string_conversion::<i32>(
            &["12:00:00", "00:00:00", "23:59:59"],
            "%H:%M:%S",
            TimeUnit::Second,
        );
    }

    #[test]
    fn millisecond() {
        assert_times_string_conversion::<i32>(
            &["12:00:00", "00:00:00", "23:59:59"],
            "%H:%M:%S",
            TimeUnit::Millisecond,
        );
    }

    #[test]
    fn microsecond() {
        assert_times_string_conversion::<i64>(
            &["12:00:00", "00:00:00", "23:59:59"],
            "%H:%M:%S",
            TimeUnit::Microsecond,
        );
    }

    #[test]
    fn nanosecond() {
        assert_times_string_conversion::<i64>(
            &["12:00:00", "00:00:00", "23:59:59"],
            "%H:%M:%S",
            TimeUnit::Nanosecond,
        );
    }

    #[test]
    fn millisecond_subsecond() {
        assert_times_string_conversion::<i32>(
            &["12:00:00.100", "00:00:00.120", "23:59:59.123"],
            "%H:%M:%S%.f",
            TimeUnit::Millisecond,
        );
    }

    trait TimePrimitive:
        Sized + TryFrom<i64, Error: std::fmt::Debug> + std::fmt::Debug + std::cmp::PartialEq + Clone
    {
        const DATA_TYPE_VARIANT: fn(TimeUnit) -> DataType;
        const ARRAY_VARIANT: fn(TimeArray<Self>) -> Array;

        fn unwrap(array: Array) -> TimeArray<Self>;
    }

    impl TimePrimitive for i32 {
        const DATA_TYPE_VARIANT: fn(TimeUnit) -> DataType = DataType::Time32;
        const ARRAY_VARIANT: fn(TimeArray<Self>) -> Array = Array::Time32;

        fn unwrap(array: Array) -> TimeArray<Self> {
            let Array::Time32(array) = array else {
                panic!();
            };
            array
        }
    }

    impl TimePrimitive for i64 {
        const DATA_TYPE_VARIANT: fn(TimeUnit) -> DataType = DataType::Time64;
        const ARRAY_VARIANT: fn(TimeArray<Self>) -> Array = Array::Time64;

        fn unwrap(array: Array) -> TimeArray<Self> {
            let Array::Time64(array) = array else {
                panic!();
            };
            array
        }
    }

    fn assert_times_string_conversion<T: TimePrimitive>(
        strings: &[&str],
        format: &str,
        unit: TimeUnit,
    ) {
        let items = strings
            .iter()
            .map(|s| Item(String::from(*s)))
            .collect::<Vec<_>>();
        let times = strings
            .iter()
            .map(|s| time::<T>(s, format, unit))
            .collect::<Vec<_>>();

        assert_deserialization::<T>(&items, &times, unit);
        assert_serialization::<T>(&items, &times, unit);
    }

    fn assert_deserialization<T: TimePrimitive>(
        items: &[Item<String>],
        times: &[T],
        unit: TimeUnit,
    ) {
        let arrays = vec![T::ARRAY_VARIANT(TimeArray {
            unit,
            validity: None,
            values: times.to_vec(),
        })];
        let views = vec![arrays[0].as_view()];

        let deserializer = Deserializer::from_marrow(&[field::<T>(unit)], &views).unwrap();
        let actual = Vec::<Item<String>>::deserialize(deserializer).unwrap();

        assert_eq!(actual, items);
    }

    fn assert_serialization<T: TimePrimitive>(items: &[Item<String>], times: &[T], unit: TimeUnit) {
        let fields = vec![field::<T>(unit)];
        let mut builder = ArrayBuilder::from_marrow(&fields).unwrap();
        items.serialize(Serializer::new(&mut builder)).unwrap();

        let arrays = builder.to_marrow().unwrap();
        let Ok([array]) = <[Array; 1]>::try_from(arrays) else {
            panic!();
        };
        let array = T::unwrap(array);

        assert_eq!(array.values, times);
    }

    fn time<T: TimePrimitive>(s: &str, format: &str, unit: TimeUnit) -> T {
        let t = NaiveTime::parse_from_str(s, format).unwrap();
        match unit {
            TimeUnit::Second => T::try_from(t.num_seconds_from_midnight() as i64).unwrap(),
            TimeUnit::Millisecond => T::try_from(
                t.num_seconds_from_midnight() as i64 * 1_000 + t.nanosecond() as i64 / 1_000_000,
            )
            .unwrap(),
            TimeUnit::Microsecond => T::try_from(
                t.num_seconds_from_midnight() as i64 * 1_000_000 + t.nanosecond() as i64 / 1_000,
            )
            .unwrap(),
            TimeUnit::Nanosecond => T::try_from(
                t.num_seconds_from_midnight() as i64 * 1_000_000_000 + t.nanosecond() as i64,
            )
            .unwrap(),
        }
    }

    fn field<T: TimePrimitive>(unit: TimeUnit) -> Field {
        Field {
            name: String::from("item"),
            data_type: T::DATA_TYPE_VARIANT(unit),
            nullable: false,
            metadata: Default::default(),
        }
    }
}
