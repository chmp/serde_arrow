use chrono::{NaiveDate, NaiveDateTime};
use marrow::{
    array::{Array, PrimitiveArray},
    datatypes::{DataType, Field},
};
use serde::{Deserialize, Serialize};

use crate::{utils::Item, ArrayBuilder, Deserializer, Serializer};

mod time_string_conversion {
    use super::*;

    #[test]
    fn date32() {
        assert_times_string_conversion::<i32>(&["2025-01-20", "1835-07-23"]);
    }

    #[test]
    fn date64() {
        assert_times_string_conversion::<i64>(&["2025-01-20", "1835-07-23"]);
    }

    trait DatePrimitive:
        TryFrom<i64, Error: std::fmt::Debug>
        + Clone
        + Sized
        + std::fmt::Debug
        + std::cmp::PartialEq
        + std::ops::Mul<Self, Output = Self>
    {
        const DAYS_TO_VALUE_FACTOR: Self;
        const ARRAY_VARIANT: fn(PrimitiveArray<Self>) -> Array;
        const DATA_TYPE: DataType;

        fn unwrap(array: Array) -> PrimitiveArray<Self>;
    }

    impl DatePrimitive for i32 {
        const DAYS_TO_VALUE_FACTOR: Self = 1;
        const ARRAY_VARIANT: fn(PrimitiveArray<Self>) -> Array = Array::Date32;
        const DATA_TYPE: DataType = DataType::Date32;

        fn unwrap(array: Array) -> PrimitiveArray<Self> {
            let Array::Date32(array) = array else {
                panic!();
            };
            array
        }
    }

    impl DatePrimitive for i64 {
        const DAYS_TO_VALUE_FACTOR: Self = 86_400_000;
        const ARRAY_VARIANT: fn(PrimitiveArray<Self>) -> Array = Array::Date64;
        const DATA_TYPE: DataType = DataType::Date64;

        fn unwrap(array: Array) -> PrimitiveArray<Self> {
            let Array::Date64(array) = array else {
                panic!();
            };
            array
        }
    }

    fn assert_times_string_conversion<T: DatePrimitive>(strings: &[&str]) {
        let items = strings
            .iter()
            .map(|s| Item(String::from(*s)))
            .collect::<Vec<_>>();
        let times = strings.iter().map(|s| date::<T>(s)).collect::<Vec<_>>();

        assert_deserialization::<T>(&items, &times);
        assert_serialization::<T>(&items, &times);
    }

    fn assert_deserialization<T: DatePrimitive>(items: &[Item<String>], times: &[T]) {
        let arrays = vec![T::ARRAY_VARIANT(PrimitiveArray {
            validity: None,
            values: times.to_vec(),
        })];
        let views = vec![arrays[0].as_view()];

        let deserializer = Deserializer::from_marrow(&[field::<T>()], &views).unwrap();
        let actual = Vec::<Item<String>>::deserialize(deserializer).unwrap();

        assert_eq!(actual, items);
    }

    fn assert_serialization<T: DatePrimitive>(items: &[Item<String>], times: &[T]) {
        let fields = vec![field::<T>()];
        let mut builder = ArrayBuilder::from_marrow(&fields).unwrap();
        items.serialize(Serializer::new(&mut builder)).unwrap();

        let arrays = builder.to_marrow().unwrap();
        let Ok([array]) = <[Array; 1]>::try_from(arrays) else {
            panic!();
        };
        let array = T::unwrap(array);

        assert_eq!(array.values, times);
    }

    fn field<T: DatePrimitive>() -> Field {
        Field {
            name: String::from("item"),
            nullable: false,
            metadata: Default::default(),
            data_type: T::DATA_TYPE,
        }
    }

    fn date<T: DatePrimitive>(s: &str) -> T {
        #[allow(deprecated)]
        const UNIX_EPOCH: NaiveDate = NaiveDateTime::UNIX_EPOCH.date();
        T::try_from((NaiveDate::parse_from_str(s, "%Y-%m-%d").unwrap() - UNIX_EPOCH).num_days())
            .unwrap()
            * T::DAYS_TO_VALUE_FACTOR
    }
}
