use std::collections::BTreeMap;

use serde::Serialize;

use super::{array_builder::ArrayBuilder, utils::Mut};

macro_rules! btree_map {
    () => {
        ::std::collections::BTreeMap::new()
    };
    ($($key:expr => $value:expr),*) => {
        {
            let mut m = ::std::collections::BTreeMap::new();
            $(m.insert($key.into(), $value.into());)*
            m
        }
    };
}

pub(crate) use btree_map;

#[test]
fn i8_array() {
    let items: Vec<i8> = vec![4, 5, 6];

    let mut builder = ArrayBuilder::list(ArrayBuilder::i8(false), false);
    items.serialize(Mut(&mut builder)).unwrap();

    let (_, offsets, element) = builder.into_list().unwrap();
    let (_, buffer) = element.into_i8().unwrap();

    assert_eq!(&offsets, &[0, 3]);
    assert_eq!(&buffer, &[4, 5, 6]);
}

#[test]
fn utf8_array() {
    let items: Vec<&str> = vec!["hello", "world"];

    let mut builder = ArrayBuilder::list(ArrayBuilder::utf8(false), false);
    items.serialize(Mut(&mut builder)).unwrap();

    let (_, offsets, element) = builder.into_list().unwrap();
    let (_, str_offsets, str_buffer) = element.into_utf8().unwrap();

    assert_eq!(&offsets, &[0, 2]);
    assert_eq!(
        &str_offsets,
        &[0, "hello".len() as i32, "helloworld".len() as i32]
    );
    assert_eq!(&str_buffer, b"helloworld");
}

#[test]
fn struct_array() {
    #[derive(Serialize)]
    struct S {
        a: i8,
        b: i8,
    }

    let items = vec![S { a: 3, b: 4 }, S { a: 5, b: 6 }];

    let mut builder = ArrayBuilder::list(
        ArrayBuilder::r#struct(
            vec![
                (String::from("a"), ArrayBuilder::i8(false)),
                (String::from("b"), ArrayBuilder::i8(false)),
            ],
            false,
        )
        .unwrap(),
        false,
    );
    items.serialize(Mut(&mut builder)).unwrap();

    let (_, offsets, element) = builder.into_list().unwrap();
    let (_, names, fields) = element.into_struct().unwrap();

    assert_eq!(&names[0], "a");
    assert_eq!(&names[1], "b");

    let (_, buffer_a) = fields[0].clone().into_i8().unwrap();
    let (_, buffer_b) = fields[1].clone().into_i8().unwrap();

    assert_eq!(offsets, vec![0, 2]);
    assert_eq!(buffer_a, vec![3, 5]);
    assert_eq!(buffer_b, vec![4, 6]);
}

#[test]
fn tuple_struct_array() {
    let items: Vec<(i32, u8)> = vec![(-13, 21), (-26, 42)];

    let mut builder = ArrayBuilder::list(
        ArrayBuilder::r#struct(
            vec![
                (String::from("0"), ArrayBuilder::i32(false)),
                (String::from("1"), ArrayBuilder::u8(false)),
            ],
            false,
        )
        .unwrap(),
        false,
    );
    items.serialize(Mut(&mut builder)).unwrap();

    let (_, offsets, element) = builder.into_list().unwrap();
    let (_, _, fields) = element.into_struct().unwrap();
    let (_, buffer_a) = fields[0].clone().into_i32().unwrap();
    let (_, buffer_b) = fields[1].clone().into_u8().unwrap();

    assert_eq!(offsets, vec![0, 2]);
    assert_eq!(buffer_a, vec![-13, -26]);
    assert_eq!(buffer_b, vec![21, 42]);
}

#[test]
fn map_array() {
    let items: Vec<BTreeMap<String, i8>> =
        vec![btree_map!("foo" => 0, "bar" => 1), btree_map!("baz" => 2)];

    let mut builder = ArrayBuilder::list(
        ArrayBuilder::map(ArrayBuilder::utf8(false), ArrayBuilder::i8(false), false),
        false,
    );

    items.serialize(Mut(&mut builder)).unwrap();

    let (_, outer_offsets, element) = builder.into_list().unwrap();
    let (_, offsets, keys, values) = element.into_map().unwrap();
    let (_, keys_offsets, keys_data) = keys.into_utf8().unwrap();
    let (_, values_data) = values.into_i8().unwrap();

    assert_eq!(outer_offsets, vec![0, 2]);
    assert_eq!(offsets, vec![0, 2, 3]);
    assert_eq!(keys_offsets, vec![0, 3, 6, 9]);

    // NOTE: btree maps are sorted, "bar" < "foo"
    assert_eq!(keys_data, b"barfoobaz");
    assert_eq!(values_data, vec![1, 0, 2]);
}

#[test]
fn nullable_i8() {
    let items: Vec<Option<i8>> = vec![None, Some(1), None, Some(2)];

    let mut builder = ArrayBuilder::list(ArrayBuilder::i8(true), false);
    items.serialize(Mut(&mut builder)).unwrap();

    let (_, _, element) = builder.into_list().unwrap();
    let (validity, data) = element.into_i8().unwrap();

    assert_eq!(validity.unwrap().as_bool(), vec![false, true, false, true]);
    assert_eq!(&data, &[0, 1, 0, 2]);
}

#[test]
fn nullable_i8_no_option() {
    let items: Vec<i8> = vec![1, 2, 3, 4];

    let mut builder = ArrayBuilder::list(ArrayBuilder::i8(true), false);
    items.serialize(Mut(&mut builder)).unwrap();

    let (_, _, element) = builder.into_list().unwrap();
    let (validity, data) = element.into_i8().unwrap();

    assert_eq!(validity.unwrap().as_bool(), vec![true, true, true, true]);
    assert_eq!(&data, &[1, 2, 3, 4]);
}
