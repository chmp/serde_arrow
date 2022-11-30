//! Test round trips on the individual array level without the out records
//!

use std::{collections::BTreeMap, fmt::Debug};

use arrow2::datatypes::{DataType, Field, UnionMode};
use serde::{Deserialize, Serialize};

use crate::{
    arrow2::{sinks::build_dynamic_array_builder, sources::build_dynamic_source},
    base::{deserialize_from_source, serialize_into_sink, Event},
    generic::{
        schema::{FieldBuilder, Tracer},
        sinks::ArrayBuilder,
    },
    test::utils::collect_events,
    Strategy, STRATEGY_KEY,
};

/// Helper to define the metadata for the given strategy
fn strategy_meta(strategy: Strategy) -> BTreeMap<String, String> {
    let mut meta = BTreeMap::new();
    meta.insert(STRATEGY_KEY.to_string(), strategy.to_string());
    meta
}

macro_rules! test_round_trip {
    (
        test_name = $test_name:ident,
        field = $field:expr,
        ty = $ty:ty,
        values = $values:expr,
        $(define = { $($items:item)* } ,)?
    ) => {
        #[test]
        fn $test_name() {
            $($($items)*)?

            let items: &[$ty] = &$values;

            let field = $field;

            let mut tracer = Tracer::new();
            for item in items {
                serialize_into_sink(&mut tracer, &item).unwrap();
            }
            let res_field = tracer.to_field("value").unwrap();
            assert_eq!(res_field, field);

            let mut sink = build_dynamic_array_builder(&field).unwrap();
            for item in items {
                serialize_into_sink(&mut sink, &item).unwrap();
            }
            let array = sink.into_array().unwrap();

            let source = build_dynamic_source(&field, array.as_ref()).unwrap();
            let events = collect_events(source).unwrap();

            // add the outer sequence
            let events = {
                let mut events = events;
                events.insert(0, Event::StartSequence);
                events.push(Event::EndSequence);
                events
            };

            let res_items: Vec<$ty> = deserialize_from_source(&events).unwrap();
            assert_eq!(res_items, items);
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
        false
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
    ]), true).with_metadata(strategy_meta(Strategy::Tuple)),
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
            .with_metadata(strategy_meta(Strategy::Tuple)),
        Field::new("1", DataType::Int64, false),
    ]), true).with_metadata(strategy_meta(Strategy::Tuple)),
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
            UnionMode::Dense
        ),
        false
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
                    false
                )
                .with_metadata(strategy_meta(Strategy::Tuple)),
                Field::new(
                    "B",
                    DataType::Struct(vec![
                        Field::new("0", DataType::UInt16, false),
                        Field::new("1", DataType::UInt64, false),
                    ]),
                    false
                )
                .with_metadata(strategy_meta(Strategy::Tuple)),
            ],
            None,
            UnionMode::Dense
        ),
        false
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
                    false
                ),
                Field::new(
                    "B",
                    DataType::Struct(vec![
                        Field::new("c", DataType::UInt16, false),
                        Field::new("d", DataType::UInt64, false),
                    ]),
                    false
                ),
            ],
            None,
            UnionMode::Dense
        ),
        false
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
        false
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

