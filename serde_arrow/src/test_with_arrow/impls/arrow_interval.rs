use serde::{Deserialize, Serialize};
use serde_json::json;

use crate::{
    _impl::arrow::_raw::array::{Array, IntervalMonthDayNanoArray},
    internal::testing::assert_error_contains,
    utils::Item,
};

use super::utils::Test;

#[derive(Debug, PartialEq, Serialize, Deserialize)]
struct MonthDayNano {
    months: i32,
    days: i32,
    nanoseconds: i64,
}

fn schema() -> serde_json::Value {
    json!([{"name": "item", "data_type": "Interval(MonthDayNano)", "nullable": true}])
}

fn get_values(test: &Test) -> Vec<(i32, i32, i64)> {
    let arrays = test.arrays.arrow.as_ref().unwrap();
    let arr = arrays[0]
        .as_any()
        .downcast_ref::<IntervalMonthDayNanoArray>()
        .unwrap();
    arr.values()
        .iter()
        .map(|v| (v.months, v.days, v.nanoseconds))
        .collect()
}

#[test]
fn strings() {
    Test::new()
        .with_schema(schema())
        .serialize(&[
            Item(Some(String::from("P1Y2M3DT4H5M6.789S"))),
            Item(Some(String::from("-P1w"))),
            Item(None),
            Item(Some(String::from("PT0s"))),
        ])
        .check_nulls(&[&[false, false, true, false]])
        .also(|it| {
            assert_eq!(
                get_values(it),
                vec![
                    (14, 3, 14_706_789_000_000),
                    (0, -7, 0),
                    (0, 0, 0),
                    (0, 0, 0)
                ]
            )
        })
        .deserialize(&[
            Item(Some(String::from("P14m3dT14706.789000000s"))),
            Item(Some(String::from("-P7d"))),
            Item(None),
            Item(Some(String::from("PT0s"))),
        ]);
}

/// Structs map to the stored components and can represent components with different signs
#[test]
fn structs() {
    let items = [
        Item(MonthDayNano {
            months: 1,
            days: -2,
            nanoseconds: 3,
        }),
        Item(MonthDayNano {
            months: i32::MAX,
            days: i32::MIN,
            nanoseconds: i64::MIN,
        }),
    ];
    Test::new()
        .with_schema(schema())
        .serialize(&items)
        .also(|it| {
            assert_eq!(
                get_values(it),
                vec![(1, -2, 3), (i32::MAX, i32::MIN, i64::MIN)]
            )
        })
        .deserialize(&items);
}

#[test]
fn json_values() {
    let items = [Item(json!({"months": 1, "days": -2, "nanoseconds": 3}))];
    Test::new()
        .with_schema(schema())
        .serialize(&items)
        .also(|it| assert_eq!(get_values(it), vec![(1, -2, 3)]))
        .deserialize(&items);
}

#[test]
fn different_signs_cannot_be_deserialized_as_strings() {
    let test = Test::new()
        .with_schema(schema())
        .serialize(&[Item(MonthDayNano {
            months: 1,
            days: -1,
            nanoseconds: 0,
        })]);
    let err = crate::from_arrow::<Vec<Item<String>>, _>(
        &test.get_arrow_fields(),
        test.arrays.arrow.as_ref().unwrap(),
    )
    .unwrap_err();
    assert_error_contains(&err, "components with different signs");
    assert_error_contains(&err, "field: \"$.item\"");
    assert_error_contains(&err, "data_type: \"Interval(MonthDayNano)\"");
}

#[test]
fn invalid_values() {
    for (value, expected) in [
        (json!("1 month"), "could not parse the string as Span"),
        (
            json!("P2147483648m"),
            "months value 2147483648 is out of range",
        ),
        (
            json!("PT9223372037s"),
            "nanoseconds value 9223372037000000000 is out of range",
        ),
        (
            json!({"months": 1, "days": 2}),
            "missing field \"nanoseconds\"",
        ),
        (
            json!({"months": 1, "days": 2, "nanoseconds": 3, "weeks": 4}),
            "unknown field \"weeks\"",
        ),
    ] {
        let mut test = Test::new().with_schema(schema());
        let err = test.try_serialize_arrow(&[Item(value)]).unwrap_err();
        assert_error_contains(&err, expected);
        assert_error_contains(&err, "data_type: \"Interval(MonthDayNano)\"");
    }
}
