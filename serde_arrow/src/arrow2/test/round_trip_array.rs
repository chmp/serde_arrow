//! Test round trips on the individual array level without the out records
//!

use std::{
    collections::{BTreeMap, HashMap},
    fmt::Debug,
};

use serde::{Deserialize, Serialize};

use crate::{
    arrow2::{deserialize_from_array, serialize_into_array, serialize_into_field},
    impls::arrow2::datatypes::{DataType, Field, UnionMode},
    internal::schema::GenericField,
    schema::{Strategy, TracingOptions, STRATEGY_KEY},
};

/// Helper to define the metadata for the given strategy
fn strategy_meta(strategy: Strategy) -> BTreeMap<String, String> {
    let mut meta = BTreeMap::new();
    meta.insert(STRATEGY_KEY.to_string(), strategy.to_string());
    meta
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

macro_rules! btreemap {
    ($($key:expr => $value:expr),*) => {
        {
            #[allow(unused_mut)]
            let mut res = BTreeMap::new();
            $(res.insert($key.into(), $value.into());)*
            res
        }
    };
}

macro_rules! test_round_trip {
    (
        test_name = $test_name:ident,
        $(tracing_options = $tracing_options:expr,)?
        field = $field:expr,
        $(overwrite_field = $overwrite_field:expr,)?
        ty = $ty:ty,
        values = $values:expr,
        $(define = { $($definitions:item)* } ,)?
    ) => {
        #[allow(unused)]
        mod $test_name {
            use super::*;

            #[test]
            fn tracing() {
                $($($definitions)*)?

                let items: &[$ty] = &$values;
                let field = $field;

                #[allow(unused)]
                let options = TracingOptions::default();
                $(let options = $tracing_options;)?

                println!("{options:?}");

                let res_field = serialize_into_field(&items, "value", options).unwrap();
                assert_eq!(res_field, field);
            }

            #[test]
            fn round_trip_array() {
                $($($definitions)*)?

                let items: &[$ty] = &$values;
                let field = $field;
                $(let field = $overwrite_field;)?

                let array = serialize_into_array(&field, &items).unwrap();
                assert_eq!(field.data_type(), array.data_type());

                let res_items: Vec<$ty> = deserialize_from_array(&field, array.as_ref()).unwrap();
                assert_eq!(res_items, items);
            }

            #[test]
            fn field_to_generic_and_back() {
                $($($definitions)*)?

                let items: &[$ty] = &$values;
                let field = $field;
                $(let field = $overwrite_field;)?

                let actual = GenericField::try_from(&field).unwrap();
                let actual = Field::try_from(&actual).unwrap();

                assert_eq!(field, actual);
            }
        }
    };
}

test_round_trip!(
    test_name = primitive_i8,
    field = Field::new("value", DataType::Int8, false),
    ty = i8,
    values = [0, 1, 2],
);
test_round_trip!(
    test_name = nullable_i8,
    field = Field::new("value", DataType::Int8, true),
    ty = Option<i8>,
    values = [Some(0), None, Some(2)],
);
test_round_trip!(
    test_name = nullable_i8_only_some,
    field = Field::new("value", DataType::Int8, true),
    ty = Option<i8>,
    values = [Some(0), Some(2)],
);

test_round_trip!(
    test_name = primitive_f32,
    field = Field::new("value", DataType::Float32, false),
    ty = f32,
    values = [0.0, 1.0, 2.0],
);
test_round_trip!(
    test_name = nullable_f32,
    field = Field::new("value", DataType::Float32, true),
    ty = Option<f32>,
    values = [Some(0.0), None, Some(2.0)],
);
test_round_trip!(
    test_name = nullable_f32_only_some,
    field = Field::new("value", DataType::Float32, true),
    ty = Option<f32>,
    values = [Some(0.0), Some(2.0)],
);

test_round_trip!(
    test_name = primitive_bool,
    field = Field::new("value", DataType::Boolean, false),
    ty = bool,
    values = [true, false, true],
);
test_round_trip!(
    test_name = nullable_bool,
    field = Field::new("value", DataType::Boolean, true),
    ty = Option<bool>,
    values = [Some(true), None, Some(false)],
);
test_round_trip!(
    test_name = nullable_bool_only_some,
    field = Field::new("value", DataType::Boolean, true),
    ty = Option<bool>,
    values = [Some(true), Some(false)],
);

test_round_trip!(
    test_name = primitive_f16,
    field = Field::new("value", DataType::Float32, false),
    overwrite_field = Field::new("value", DataType::Float16, false),
    ty = f32,
    values = [0.0, 1.0, 2.0],
);

test_round_trip!(
    test_name = vec_bool,
    field = Field::new(
        "value",
        DataType::LargeList(Box::new(Field::new("element", DataType::Boolean, false))),
        false,
    ),
    ty = Vec<bool>,
    values = [vec![true, false], vec![], vec![false]],
);
test_round_trip!(
    test_name = nullable_vec_bool,
    field = Field::new("value", DataType::LargeList(Box::new(Field::new("element", DataType::Boolean, false))), true),
    ty = Option<Vec<bool>>,
    values = [Some(vec![true, false]), None, Some(vec![])],
);
test_round_trip!(
    test_name = nullable_vec_bool_nested,
    field = Field::new(
        "value",
        DataType::LargeList(Box::new(Field::new(
            "element",
            DataType::LargeList(Box::new(Field::new("element", DataType::Boolean, false))),
            false),
        )),
        true,
    ),
    ty = Option<Vec<Vec<bool>>>,
    values = [Some(vec![vec![true], vec![false, false]]), None, Some(vec![vec![]])],
);
test_round_trip!(
    test_name = vec_nullable_bool,
    field = Field::new("value", DataType::LargeList(Box::new(Field::new("element", DataType::Boolean, true))), false),
    ty = Vec<Option<bool>>,
    values = [vec![Some(true), Some(false)], vec![], vec![None, Some(false)]],
);

test_round_trip!(
    test_name = struct_nullable,
    field = Field::new("value",DataType::Struct(vec![
        Field::new("a", DataType::Boolean, false),
        Field::new("b", DataType::Int64, false),
        Field::new("c", DataType::Null, true),
        Field::new("d", DataType::LargeUtf8, false),
    ]), true),
    ty = Option<Struct>,
    values = [
        Some(Struct {
            a: true,
            b: 42,
            c: (),
            d: String::from("hello"),
        }),
        None,
        Some(Struct {
            a: false,
            b: 13,
            c: (),
            d: String::from("world"),
        }),
    ],
    define = {
        #[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
        struct Outer {
            inner: Struct,
        }

        #[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
        struct Struct {
            a: bool,
            b: i64,
            c: (),
            d: String,
        }
    },
);

test_round_trip!(
    test_name = struct_nullable_nested,
    field = Field::new("value",DataType::Struct(vec![
        Field::new("inner", DataType::Struct(vec![
            Field::new("a", DataType::Boolean, false),
            Field::new("b", DataType::Int64, false),
            Field::new("c", DataType::Null, true),
            Field::new("d", DataType::LargeUtf8, false),
        ]), false),
    ]),true),
    ty = Option<Outer>,
    values = [
        Some(Outer {
            inner: Struct {
            a: true,
            b: 42,
            c: (),
            d: String::from("hello"),
        }}),
        None,
        Some(Outer {inner: Struct {
            a: false,
            b: 13,
            c: (),
            d: String::from("world"),
        }}),
    ],
    define = {
        #[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
        struct Outer {
            inner: Struct,
        }

        #[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
        struct Struct {
            a: bool,
            b: i64,
            c: (),
            d: String,
        }
    },
);

test_round_trip!(
    test_name = struct_nullable_item,
    field = Field::new(
        "value",
        DataType::Struct(vec![
            Field::new("a", DataType::Boolean, true),
            Field::new("b", DataType::Int64, true),
            Field::new("c", DataType::Null, true),
            Field::new("d", DataType::LargeUtf8, true),
        ]),
        false,
    ),
    ty = StructNullable,
    values = [
        StructNullable {
            a: None,
            b: None,
            c: None,
            d: Some(String::from("hello")),
        },
        StructNullable {
            a: Some(true),
            b: Some(42),
            c: None,
            d: None,
        },
    ],
    define = {
        #[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
        struct StructNullable {
            a: Option<bool>,
            b: Option<i64>,
            c: Option<()>,
            d: Option<String>,
        }
    },
);

test_round_trip!(
    test_name = tuple_nullable,
    field = Field::new("value", DataType::Struct(vec![
        Field::new("0", DataType::Boolean, false),
        Field::new("1", DataType::Int64, false),
    ]), true).with_metadata(strategy_meta(Strategy::TupleAsStruct)),
    ty = Option<(bool, i64)>,
    values = [
        Some((true, 21)),
        None,
        Some((false, 42)),
    ],
);

test_round_trip!(
    test_name = tuple_nullable_nested,
    field = Field::new("value", DataType::Struct(vec![
        Field::new("0", DataType::Struct(vec![
                Field::new("0", DataType::Boolean, false),
                Field::new("1", DataType::Int64, false),
            ]), false)
            .with_metadata(strategy_meta(Strategy::TupleAsStruct)),
        Field::new("1", DataType::Int64, false),
    ]), true).with_metadata(strategy_meta(Strategy::TupleAsStruct)),
    ty = Option<((bool, i64), i64)>,
    values = [
        Some(((true, 21), 7)),
        None,
        Some(((false, 42), 13)),
    ],
);

test_round_trip!(
    test_name = enums,
    field = Field::new(
        "value",
        DataType::Union(
            vec![
                Field::new("U8", DataType::UInt8, false),
                Field::new("U16", DataType::UInt16, false),
                Field::new("U32", DataType::UInt32, false),
                Field::new("U64", DataType::UInt64, false),
            ],
            None,
            UnionMode::Dense,
        ),
        false,
    ),
    ty = Item,
    values = [Item::U32(2), Item::U64(3), Item::U8(0), Item::U16(1),],
    define = {
        #[derive(Debug, PartialEq, Deserialize, Serialize)]
        enum Item {
            U8(u8),
            U16(u16),
            U32(u32),
            U64(u64),
        }
    },
);

test_round_trip!(
    test_name = enums_tuple,
    field = Field::new(
        "value",
        DataType::Union(
            vec![
                Field::new(
                    "A",
                    DataType::Struct(vec![
                        Field::new("0", DataType::UInt8, false),
                        Field::new("1", DataType::UInt32, false),
                    ]),
                    false,
                )
                .with_metadata(strategy_meta(Strategy::TupleAsStruct)),
                Field::new(
                    "B",
                    DataType::Struct(vec![
                        Field::new("0", DataType::UInt16, false),
                        Field::new("1", DataType::UInt64, false),
                    ]),
                    false,
                )
                .with_metadata(strategy_meta(Strategy::TupleAsStruct)),
            ],
            None,
            UnionMode::Dense,
        ),
        false,
    ),
    ty = Item,
    values = [Item::A(2, 3), Item::B(0, 1),],
    define = {
        #[derive(Debug, PartialEq, Deserialize, Serialize)]
        enum Item {
            A(u8, u32),
            B(u16, u64),
        }
    },
);

test_round_trip!(
    test_name = enums_struct,
    field = Field::new(
        "value",
        DataType::Union(
            vec![
                Field::new(
                    "A",
                    DataType::Struct(vec![
                        Field::new("a", DataType::UInt8, false),
                        Field::new("b", DataType::UInt32, false),
                    ]),
                    false,
                ),
                Field::new(
                    "B",
                    DataType::Struct(vec![
                        Field::new("c", DataType::UInt16, false),
                        Field::new("d", DataType::UInt64, false),
                    ]),
                    false,
                ),
            ],
            None,
            UnionMode::Dense,
        ),
        false,
    ),
    ty = Item,
    values = [Item::A { a: 2, b: 3 }, Item::B { c: 0, d: 1 },],
    define = {
        #[derive(Debug, PartialEq, Deserialize, Serialize)]
        enum Item {
            A { a: u8, b: u32 },
            B { c: u16, d: u64 },
        }
    },
);

test_round_trip!(
    test_name = enums_union,
    field = Field::new(
        "value",
        DataType::Union(
            vec![
                Field::new("A", DataType::Null, true),
                Field::new("B", DataType::Null, true),
            ],
            None,
            UnionMode::Dense
        ),
        false,
    ),
    ty = Item,
    values = [Item::A, Item::B,],
    define = {
        #[derive(Debug, PartialEq, Deserialize, Serialize)]
        enum Item {
            A,
            B,
        }
    },
);

test_round_trip!(
    test_name = hash_maps,
    tracing_options = TracingOptions::new().map_as_struct(false),
    field = Field::new(
        "value",
        DataType::Map(
            Box::new(Field::new(
                "entries",
                DataType::Struct(vec![
                    Field::new("key", DataType::Int64, false),
                    Field::new("value", DataType::Boolean, false),
                ]),
                false
            )),
            false,
        ),
        false,
    ),
    ty = HashMap<i64, bool>,
    values = [
        hashmap!{0 => true, 1 => false, 2 => true},
        hashmap!{3 => false, 4 => true},
        hashmap!{},
    ],
);

test_round_trip!(
    test_name = hash_maps_nullable,
    tracing_options = TracingOptions::new().map_as_struct(false),
    field = Field::new(
        "value",
        DataType::Map(
            Box::new(Field::new(
                "entries",
                DataType::Struct(vec![
                    Field::new("key", DataType::Int64, false),
                    Field::new("value", DataType::Boolean, false),
                ]),
                false
            )),
            false,
        ),
        true,
    ),
    ty = Option<HashMap<i64, bool>>,
    values = [
        Some(hashmap!{0 => true, 1 => false, 2 => true}),
        Some(hashmap!{3 => false, 4 => true}),
        Some(hashmap!{}),
    ],
);

test_round_trip!(
    test_name = hash_maps_nullable_keys,
    tracing_options = TracingOptions::new().map_as_struct(false),
    field = Field::new(
        "value",
        DataType::Map(
            Box::new(Field::new(
                "entries",
                DataType::Struct(vec![
                    Field::new("key", DataType::Int64, true),
                    Field::new("value", DataType::Boolean, false),
                ]),
                false
            )),
            false,
        ),
        false,
    ),
    ty = HashMap<Option<i64>, bool>,
    values = [
        hashmap!{Some(0) => true, Some(1) => false, Some(2) => true},
        hashmap!{Some(3) => false, Some(4) => true},
        hashmap!{},
    ],
);

test_round_trip!(
    test_name = hash_maps_nullable_values,
    tracing_options = TracingOptions::new().map_as_struct(false),
    field = Field::new(
        "value",
        DataType::Map(
            Box::new(Field::new(
                "entries",
                DataType::Struct(vec![
                    Field::new("key", DataType::Int64, false),
                    Field::new("value", DataType::Boolean, true),
                ]),
                false
            )),
            false,
        ),
        false,
    ),
    ty = HashMap<i64, Option<bool>>,
    values = [
        hashmap!{0 => Some(true), 1 => Some(false), 2 => Some(true)},
        hashmap!{3 => Some(false), 4 => Some(true)},
        hashmap!{},
    ],
);

test_round_trip!(
    test_name = btree_maps,
    tracing_options = TracingOptions::new().map_as_struct(false),
    field = Field::new(
        "value",
        DataType::Map(
            Box::new(Field::new(
                "entries",
                DataType::Struct(vec![
                    Field::new("key", DataType::Int64, false),
                    Field::new("value", DataType::Boolean, false),
                ]),
                false
            )),
            false,
        ),
        false,
    ),
    ty = BTreeMap<i64, bool>,
    values = [
        btreemap!{0 => true, 1 => false, 2 => true},
        btreemap!{3 => false, 4 => true},
        btreemap!{},
    ],
);

test_round_trip!(
    test_name = flattened_structures,
    field = Field::new(
        "value",
        DataType::Struct(vec![
            Field::new("a", DataType::Int64, false),
            Field::new("b", DataType::Float32, false),
            Field::new("c", DataType::Float64, false),
        ]),
        false,
    )
    .with_metadata(strategy_meta(Strategy::MapAsStruct)),
    ty = Outer,
    values = [
        Outer {
            a: 0,
            inner: Inner { b: 1.0, c: 2.0 }
        },
        Outer {
            a: 3,
            inner: Inner { b: 4.0, c: 5.0 }
        },
        Outer {
            a: 6,
            inner: Inner { b: 7.0, c: 8.0 }
        },
    ],
    define = {
        #[derive(Debug, PartialEq, Serialize, Deserialize)]
        struct Outer {
            a: i64,
            #[serde(flatten)]
            inner: Inner,
        }

        #[derive(Debug, PartialEq, Serialize, Deserialize)]
        struct Inner {
            b: f32,
            c: f64,
        }
    },
);
