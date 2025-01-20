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
    fn microsecond() {
        assert_times_string_conversion(
            &["12:00:00", "00:00:00", "23:59:59"],
            TimeUnit::Microsecond,
        );
    }

    #[test]
    fn nanosecond() {
        assert_times_string_conversion(&["12:00:00", "00:00:00", "23:59:59"], TimeUnit::Nanosecond);
    }

    fn assert_times_string_conversion(strings: &[&str], unit: TimeUnit) {
        let items = strings
            .iter()
            .map(|s| Item(String::from(*s)))
            .collect::<Vec<_>>();
        let times = strings.iter().map(|s| time(*s, unit)).collect::<Vec<_>>();

        assert_deserialization(&items, &times, unit);
        assert_serialization(&items, &times, unit);
    }

    fn assert_deserialization(items: &[Item<String>], times: &[i64], unit: TimeUnit) {
        let arrays = vec![Array::Time64(TimeArray {
            unit,
            validity: None,
            values: times.to_vec(),
        })];
        let views = vec![arrays[0].as_view()];

        let deserializer = Deserializer::from_marrow(&[field(unit)], &views).unwrap();
        let actual = Vec::<Item<String>>::deserialize(deserializer).unwrap();

        assert_eq!(actual, items);
    }

    fn assert_serialization(items: &[Item<String>], times: &[i64], unit: TimeUnit) {
        let fields = vec![field(unit)];
        let mut builder = ArrayBuilder::from_marrow(&fields).unwrap();
        items.serialize(Serializer::new(&mut builder)).unwrap();

        let arrays = builder.to_marrow().unwrap();
        let Ok([Array::Time64(array)]) = <[Array; 1]>::try_from(arrays) else {
            panic!();
        };

        assert_eq!(array.values, times);
    }

    fn time(s: &str, unit: TimeUnit) -> i64 {
        let t = NaiveTime::parse_from_str(s, "%H:%M:%S").unwrap();
        match unit {
            TimeUnit::Second => t.num_seconds_from_midnight() as i64,
            TimeUnit::Millisecond => {
                t.num_seconds_from_midnight() as i64 * 1_000 + t.nanosecond() as i64 / 1_000_000
            }
            TimeUnit::Microsecond => {
                t.num_seconds_from_midnight() as i64 * 1_000_000 + t.nanosecond() as i64 / 1_000
            }
            TimeUnit::Nanosecond => {
                t.num_seconds_from_midnight() as i64 * 1_000_000_000 + t.nanosecond() as i64
            }
        }
    }

    fn field(unit: TimeUnit) -> Field {
        Field {
            name: String::from("item"),
            data_type: DataType::Time64(unit),
            nullable: false,
            metadata: Default::default(),
        }
    }
}
