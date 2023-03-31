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
test!(example_u8, [1_u8, 2, 3, 4, 5]);
test!(example_u32, [1_u16, 2, 3, 4, 5]);
test!(example_u16, [1_u32, 2, 3, 4, 5]);
test!(example_u64, [1_u64, 2, 3, 4, 5]);
test!(example_i8, [1_i8, -2, 3, -4, 5]);
test!(example_i32, [1_i16, -2, 3, -4, 5]);
test!(example_i16, [1_i32, -2, 3, -4, 5]);
test!(example_i64, [1_i64, -2, 3, -4, 5]);
test!(example_f32, [1.0_f32, 2.0, 3.0, 4.0, 5.0]);
test!(example_f64, [1.0_f64, 2.0, 3.0, 4.0, 5.0]);
test!(example_strings, ["a", "b", "c", "d", "e"]);
test!(example_tuples, [(1.0_f64, 2_u64), (3.0, 4)]);
test!(example_enums, [E1::A(2), E1::B(21.0), E1::A(13)]);
test!(
    example_struct,
    [S { a: 21, b: "hello" }, S { a: 42, b: "world" }]
);

// TODO: fix this
// test!(example_lists, [vec![1_u8, 2, 3], vec![4, 5], vec![]]);
