use std::collections::HashMap;

use serde::Serialize;

use crate::{
    arrow::{serialize_into_array, serialize_into_field},
    schema::TracingOptions,
};

macro_rules! test {
    ($name:ident, $items:expr) => {
        #[test]
        fn $name() {
            let items = &$items;

            let field = serialize_into_field(&items, "root", TracingOptions::default()).unwrap();
            let array = serialize_into_array(&field, &items).unwrap();

            assert_eq!(array.len(), items.len());
            assert_eq!(array.data_type(), field.data_type());
        }
    };
}

macro_rules! hashmap {
    ($($key:expr => $value:expr),*) => {
        {
            #[allow(unused_mut)]
            let mut res = HashMap::new();
            $(res.insert($key.into(), $value.into());)*
            res
        }
    };
}

#[derive(Serialize)]
struct S {
    a: u32,
    b: &'static str,
}

#[derive(Serialize)]
enum E1 {
    A(u32),
    B(f32),
}

test!(example_bool, [true, false]);
test!(example_bool_opt, [Some(true), None]);
test!(example_u8, [1_u8, 2, 3, 4, 5]);
test!(example_u8_opt, [Some(1_u8), None, Some(3), None, Some(5)]);
test!(example_u16, [1_u32, 2, 3, 4, 5]);
test!(example_u16_opt, [Some(1_u16), None, Some(3), None, Some(5)]);
test!(example_u32, [1_u32, 2, 3, 4, 5]);
test!(example_u32_opt, [Some(1_u32), None, Some(3), None, Some(5)]);
test!(example_u64, [1_u64, 2, 3, 4, 5]);
test!(example_u64_opt, [Some(1_u64), None, Some(3), None, Some(5)]);
test!(example_i8, [1_i8, -2, 3, -4, 5]);
test!(example_i8_opt, [Some(1_i8), None, Some(3), None, Some(5)]);
test!(example_i16, [1_i32, -2, 3, -4, 5]);
test!(example_i16_opt, [Some(1_i16), None, Some(3), None, Some(5)]);
test!(example_i32, [1_i16, -2, 3, -4, 5]);
test!(example_i32_opt, [Some(1_i32), None, Some(3), None, Some(5)]);
test!(example_i64, [1_i64, -2, 3, -4, 5]);
test!(example_i64_opt, [Some(1_i64), None, Some(3), None, Some(5)]);
test!(example_f32, [1.0_f32, 2.0, 3.0, 4.0, 5.0]);
test!(example_f32_opt, [None, Some(1.0_f32), Some(2.0)]);
test!(example_f64, [1.0_f64, 2.0, 3.0, 4.0, 5.0]);
test!(example_f64_opt, [Some(1.0_f64), None, Some(3.0)]);
test!(example_strings, ["a", "b", "c", "d", "e"]);
test!(example_strings_opt, [Some("a"), Some("b"), None, None]);
test!(example_tuples, [(1.0_f64, 2_u64), (3.0, 4)]);
test!(example_tuples_opt, [Some((1.0_f64, 2_u64)), None]);
test!(
    example_struct,
    [S { a: 21, b: "hello" }, S { a: 42, b: "world" }]
);
test!(
    example_struct_opt,
    [
        Some(S { a: 21, b: "hello" }),
        Some(S { a: 42, b: "world" }),
        None
    ]
);
test!(example_enums, [E1::A(2), E1::B(21.0), E1::A(13)]);
test!(example_lists, [vec![1_u8, 2, 3], vec![4, 5], vec![]]);
test!(
    example_lists_opt,
    [Some(vec![1_u8, 2, 3]), None, Some(vec![])]
);

#[test]
fn example_dictionary_str() {
    let items = &[Some("a"), Some("b"), None, Some("c"), Some("b")];

    let field = serialize_into_field(
        &items,
        "root",
        TracingOptions::default().string_dictionary_encoding(true),
    )
    .unwrap();
    let array = serialize_into_array(&field, &items).unwrap();

    assert_eq!(array.len(), items.len());

    let nulls = (0..array.len())
        .into_iter()
        .map(|idx| array.is_null(idx))
        .collect::<Vec<_>>();
    assert_eq!(nulls, vec![false, false, true, false, false]);
}

#[test]
fn example_map_ints() {
    let items: &[HashMap<u32, u64>] = &[
        hashmap!(1_u32 => 2_u64, 3_u32 => 4_u64),
        hashmap!(5_u32 => 6_u64),
    ];
    let field = serialize_into_field(
        &items,
        "root",
        TracingOptions::default().map_as_struct(false),
    )
    .unwrap();
    let array = serialize_into_array(&field, &items).unwrap();

    assert_eq!(array.len(), items.len());
}

#[test]
fn example_map_str_float() {
    let items: &[HashMap<&'static str, f32>] =
        &[hashmap!("a" => 13.0, "b" => 21.0), hashmap!("c" => 42.0)];
    let field = serialize_into_field(
        &items,
        "root",
        TracingOptions::default().map_as_struct(false),
    )
    .unwrap();
    let array = serialize_into_array(&field, &items).unwrap();

    assert_eq!(array.len(), items.len());
}

test!(
    example_list_nullable_u64,
    [
        vec![Some(1_u64), None, Some(2_u64)],
        vec![Some(4_u64), None],
        vec![]
    ]
);
