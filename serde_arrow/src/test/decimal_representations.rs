use std::str::FromStr;

use rust_decimal::Decimal;
use serde::Serialize;

use crate::internal::{event::Event, sink::serialize_into_sink};

#[test]
fn example_str() {
    #[derive(Serialize)]
    struct Wrapper {
        #[serde(with = "rust_decimal::serde::str")]
        value: Decimal,
    }

    let mut events = Vec::<Event<'_>>::new();
    serialize_into_sink(
        &mut events,
        &Wrapper {
            value: Decimal::from_str("0.20").unwrap(),
        },
    )
    .unwrap();

    assert_eq!(
        events,
        vec![
            Event::StartStruct,
            Event::Str("value").to_owned(),
            Event::Str("0.20").to_owned(),
            Event::EndStruct,
        ],
    );
}

#[test]
fn example_float() {
    #[derive(Serialize)]
    struct Wrapper {
        #[serde(with = "rust_decimal::serde::float")]
        value: Decimal,
    }

    let mut events = Vec::<Event<'_>>::new();
    serialize_into_sink(
        &mut events,
        &Wrapper {
            value: Decimal::from_str("0.20").unwrap(),
        },
    )
    .unwrap();

    assert_eq!(
        events,
        vec![
            Event::StartStruct,
            Event::Str("value").to_owned(),
            Event::F64(0.2),
            Event::EndStruct,
        ],
    );
}

/*
#[test]
fn example_arbitrary_precision() {
    #[derive(Serialize)]
    struct Wrapper {
        #[serde(with = "rust_decimal::serde::arbitrary_precision")]
        value: Decimal,
    }

    let mut events = Vec::<Event<'_>>::new();
    serialize_into_sink(&mut events, &Wrapper { value: Decimal::from_str("0.20").unwrap() }).unwrap();

    assert_eq!(
        events,
        vec![
            Event::StartStruct,
            Event::Str("value").to_owned(),
            Event::StartStruct,
            Event::Str("$serde_json::private::Number").to_owned(),
            Event::Str("0.20"),
            Event::EndStruct,
            Event::EndStruct,
        ],
    );
}
*/
