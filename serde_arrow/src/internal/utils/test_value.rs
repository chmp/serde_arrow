use serde::{de::DeserializeOwned, Deserialize, Serialize};

use crate::internal::utils::value::{ValueDeserializer, ValueSerializer};

fn roundtrip<T: Serialize + DeserializeOwned>(value: &T) -> T {
    let value = value.serialize(ValueSerializer).unwrap();
    T::deserialize(ValueDeserializer::new(&value)).unwrap()
}

#[test]
fn example() {
    #[derive(Debug, PartialEq, Serialize, Deserialize)]
    struct S {
        a: i32,
        b: i64,
    }

    let item = S { a: 13, b: 21 };
    assert_eq!(item, roundtrip(&item));
}

#[test]
fn example_i8() {
    let item: i8 = -42;
    assert_eq!(item, roundtrip(&item));
}

#[test]
fn example_i16() {
    let item: i16 = -42;
    assert_eq!(item, roundtrip(&item));
}

#[test]
fn example_i32() {
    let item: i32 = -42;
    assert_eq!(item, roundtrip(&item));
}

#[test]
fn example_i64() {
    let item: i64 = -42;
    assert_eq!(item, roundtrip(&item));
}

#[test]
fn example_u8() {
    let item: u8 = 42;
    assert_eq!(item, roundtrip(&item));
}

#[test]
fn example_u16() {
    let item: u16 = 42;
    assert_eq!(item, roundtrip(&item));
}

#[test]
fn example_u32() {
    let item: u32 = 42;
    assert_eq!(item, roundtrip(&item));
}

#[test]
fn example_u64() {
    let item: u64 = 42;
    assert_eq!(item, roundtrip(&item));
}
