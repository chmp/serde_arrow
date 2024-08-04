use super::utils::Test;
use crate::{
    internal::schema::{GenericDataType, GenericField},
    schema::{Strategy, TracingOptions, STRATEGY_KEY},
    utils::Item,
};

use serde::{Deserialize, Serialize};
use serde_json::json;

#[test]
fn benchmark_primitives() {
    #[derive(Default, Serialize, Deserialize, Debug, PartialEq)]
    struct Item {
        pub a: u8,
        pub b: u16,
        pub c: u32,
        pub d: u64,
        pub e: i8,
        pub f: i16,
        pub g: i32,
        pub h: i64,
        pub i: f32,
        pub j: f64,
        pub k: bool,
    }

    let items = [Item::default(), Item::default()];

    Test::new()
        .with_schema(json!([
            {"name": "a", "data_type": "U8"},
            {"name": "b", "data_type": "U16"},
            {"name": "c", "data_type": "U32"},
            {"name": "d", "data_type": "U64"},
            {"name": "e", "data_type": "I8"},
            {"name": "f", "data_type": "I16"},
            {"name": "g", "data_type": "I32"},
            {"name": "h", "data_type": "I64"},
            {"name": "i", "data_type": "F32"},
            {"name": "j", "data_type": "F64"},
            {"name": "k", "data_type": "Bool"},
        ]))
        .trace_schema_from_samples(&items, TracingOptions::default())
        .trace_schema_from_type::<Item>(TracingOptions::default())
        .serialize(&items)
        .deserialize(&items);
}

#[test]
fn benchmark_complex_1() {
    #[derive(Serialize, Deserialize, Debug, PartialEq)]
    struct Item {
        string: String,
        points: Vec<Point>,
        float: Float,
    }

    #[derive(Serialize, Deserialize, Debug, PartialEq)]
    enum Float {
        F32(f32),
        F64(f64),
    }

    #[derive(Serialize, Deserialize, Debug, PartialEq)]
    struct Point {
        x: f32,
        y: f32,
    }

    let items = [
        Item {
            string: "foo".into(),
            points: vec![Point { x: 0.0, y: 0.0 }],
            float: Float::F32(13.0),
        },
        Item {
            string: "foo".into(),
            points: vec![],
            float: Float::F64(21.0),
        },
    ];

    Test::new()
        .with_schema(json!([
            {"name": "string", "data_type": "LargeUtf8"},
            {
                "name": "points",
                "data_type":
                "LargeList",
                "children": [
                    {
                        "name": "element",
                        "data_type": "Struct",
                        "children": [
                            {"name": "x", "data_type": "F32"},
                            {"name": "y", "data_type": "F32"},
                        ],
                    },
                ],
            },
            {
                "name": "float",
                "data_type": "Union",
                "children": [
                    {"name": "F32", "data_type": "F32"},
                    {"name": "F64", "data_type": "F64"},
                ],
            },
        ]))
        .trace_schema_from_samples(&items, TracingOptions::default())
        .trace_schema_from_type::<Item>(TracingOptions::default())
        .serialize(&items)
        .deserialize(&items)
        .check_nulls(&[&[false, false], &[false, false], &[false, false]]);
}

#[test]
fn benchmark_complex_2() {
    #[derive(Serialize, Deserialize, Debug, PartialEq)]
    struct Item {
        string: String,
        points: Vec<Point>,
        child: SubItem,
    }

    #[derive(Serialize, Deserialize, Debug, PartialEq)]
    struct Point {
        x: f32,
        y: f32,
    }

    #[derive(Serialize, Deserialize, Debug, PartialEq)]
    struct SubItem {
        a: bool,
        b: f64,
        c: Option<f32>,
    }

    let items = [
        Item {
            string: "foo".into(),
            points: vec![Point { x: 0.0, y: 1.0 }, Point { x: 2.0, y: 3.0 }],
            child: SubItem {
                a: true,
                b: 42.0,
                c: None,
            },
        },
        Item {
            string: "bar".into(),
            points: vec![],
            child: SubItem {
                a: false,
                b: 13.0,
                c: Some(7.0),
            },
        },
    ];

    Test::new()
        .with_schema(json!([
            {"name": "string", "data_type": "LargeUtf8"},
            {
                "name": "points",
                "data_type":
                "LargeList",
                "children": [
                    {
                        "name": "element",
                        "data_type": "Struct",
                        "children": [
                            {"name": "x", "data_type": "F32"},
                            {"name": "y", "data_type": "F32"},
                        ],
                    },
                ],
            },
            {
                "name": "child",
                "data_type": "Struct",
                "children": [
                    {"name": "a", "data_type": "Bool"},
                    {"name": "b", "data_type": "F64"},
                    {"name": "c", "data_type": "F32", "nullable": true},
                ],
            },
        ]))
        .trace_schema_from_samples(&items, TracingOptions::default())
        .trace_schema_from_type::<Item>(TracingOptions::default())
        .serialize(&items)
        .deserialize(&items)
        .check_nulls(&[&[false, false], &[false, false], &[false, false]]);
}

#[test]
fn nested_options() {
    #[derive(Serialize, Deserialize, Debug, PartialEq)]
    struct Item {
        a: u8,
        b: Option<u16>,
        c: Option<Option<u32>>,
    }

    let items = [
        Item {
            a: 0,
            b: Some(1),
            c: Some(Some(2)),
        },
        Item {
            a: 0,
            b: None,
            c: Some(None),
        },
        Item {
            a: 0,
            b: None,
            c: None,
        },
    ];
    let expected_deserialized = [
        Item {
            a: 0,
            b: Some(1),
            c: Some(Some(2)),
        },
        Item {
            a: 0,
            b: None,
            // NOTE: the arrow format only has a single level of "nullness"
            // therefore `None` and `Some(None)` cannot be distinguished
            c: None,
        },
        Item {
            a: 0,
            b: None,
            c: None,
        },
    ];

    Test::new()
        .with_schema(json!([
            {"name": "a", "data_type": "U8"},
            {"name": "b", "data_type": "U16", "nullable": true},
            {"name": "c", "data_type": "U32", "nullable": true},
        ]))
        .trace_schema_from_samples(&items, TracingOptions::default())
        .trace_schema_from_type::<Item>(TracingOptions::default())
        .serialize(&items)
        .deserialize(&expected_deserialized)
        .check_nulls(&[
            &[false, false, false],
            &[false, true, true],
            &[false, true, true],
        ]);
}

#[test]
fn fieldless_unions_in_a_struct() {
    #[derive(Serialize, Deserialize, Debug, PartialEq)]
    struct S {
        foo: u32,
        bar: U,
        baz: f32,
    }

    #[derive(Serialize, Deserialize, Debug, PartialEq)]
    enum U {
        A,
        B,
        C,
    }

    let items = [
        S {
            foo: 0,
            bar: U::A,
            baz: 1.0,
        },
        S {
            foo: 2,
            bar: U::B,
            baz: 3.0,
        },
        S {
            foo: 4,
            bar: U::C,
            baz: 5.0,
        },
        S {
            foo: 6,
            bar: U::A,
            baz: 7.0,
        },
    ];

    Test::new()
        .with_schema(vec![
            GenericField::new("foo", GenericDataType::U32, false),
            GenericField::new("bar", GenericDataType::Union, false)
                .with_child(GenericField::new("A", GenericDataType::Null, true))
                .with_child(GenericField::new("B", GenericDataType::Null, true))
                .with_child(GenericField::new("C", GenericDataType::Null, true)),
            GenericField::new("baz", GenericDataType::F32, false),
        ])
        .trace_schema_from_samples(&items, TracingOptions::default().allow_null_fields(true))
        .trace_schema_from_type::<S>(TracingOptions::default().allow_null_fields(true))
        .serialize(&items)
        .deserialize(&items)
        .check_nulls(&[
            &[false, false, false, false],
            &[false, false, false, false],
            &[false, false, false, false],
        ]);
}

#[test]
fn issue_57() {
    #[derive(Debug, PartialEq, Serialize, Deserialize)]
    pub enum AccountType {
        PlayByPlay,
        Deduced,
        BoxScore,
    }

    #[derive(Debug, PartialEq, Serialize, Deserialize)]
    pub enum GameType {
        SpringTraining,
        RegularSeason,
        AllStarGame,
        WildCardSeries,
        DivisionSeries,
        LeagueChampionshipSeries,
        WorldSeries,
        NegroLeagues,
        Other,
    }

    #[derive(Debug, PartialEq, Serialize, Deserialize)]
    pub struct FileInfo {
        pub filename: String,
        pub game_type: GameType,
        pub account_type: AccountType,
        pub file_index: usize,
    }

    let items = [FileInfo {
        filename: String::from("test"),
        game_type: GameType::RegularSeason,
        account_type: AccountType::Deduced,
        file_index: 0,
    }];

    Test::new()
        .with_schema(vec![
            GenericField::new("filename", GenericDataType::LargeUtf8, false),
            GenericField::new("game_type", GenericDataType::Union, false)
                .with_child(
                    GenericField::new("", GenericDataType::Null, true).with_metadata(
                        STRATEGY_KEY.to_string(),
                        Strategy::UnknownVariant.to_string(),
                    ),
                )
                .with_child(GenericField::new(
                    "RegularSeason",
                    GenericDataType::Null,
                    true,
                )),
            GenericField::new("account_type", GenericDataType::Union, false)
                .with_child(
                    GenericField::new("", GenericDataType::Null, true).with_metadata(
                        STRATEGY_KEY.to_string(),
                        Strategy::UnknownVariant.to_string(),
                    ),
                )
                .with_child(GenericField::new("Deduced", GenericDataType::Null, true)),
            GenericField::new("file_index", GenericDataType::U64, false),
        ])
        .trace_schema_from_samples(&items, TracingOptions::default().allow_null_fields(true))
        // NOTE: trace_from_type discovers all variants
        // .trace_schema_from_type::<FileInfo>(TracingOptions::default().allow_null_fields(true))
        .serialize(&items)
        .deserialize(&items)
        .check_nulls(&[&[false], &[false], &[false], &[false]]);
}

#[test]
fn simple_example() {
    #[derive(PartialEq, Clone, Debug, Serialize, Deserialize)]
    struct S {
        a: f32,
        b: u32,
    }

    let items = &[S { a: 2.0, b: 4 }, S { a: -123.0, b: 9 }];

    Test::new()
        .with_schema(vec![
            GenericField::new("a", GenericDataType::F32, false),
            GenericField::new("b", GenericDataType::U32, false),
        ])
        .trace_schema_from_samples(items, TracingOptions::default().allow_null_fields(true))
        .serialize(items)
        .deserialize(items)
        .check_nulls(&[&[false, false], &[false, false]]);
}

#[test]
fn top_level_nullables() {
    #[derive(PartialEq, Clone, Debug, Serialize, Deserialize)]
    struct S {
        a: Option<f32>,
        b: Option<u32>,
    }

    let items = &[
        S {
            a: Some(2.0),
            b: None,
        },
        S {
            a: None,
            b: Some(9),
        },
    ];

    Test::new()
        .with_schema(vec![
            GenericField::new("a", GenericDataType::F32, true),
            GenericField::new("b", GenericDataType::U32, true),
        ])
        .trace_schema_from_samples(items, TracingOptions::default().allow_null_fields(true))
        .serialize(items)
        .deserialize(items)
        .check_nulls(&[&[false, true], &[true, false]]);
}

#[test]
fn new_type_wrappers() {
    #[derive(Debug, PartialEq, Serialize, Deserialize)]
    struct U64(u64);

    let items = [Item(U64(0)), Item(U64(1)), Item(U64(2))];

    Test::new()
        .with_schema(vec![GenericField::new("item", GenericDataType::U64, false)])
        .trace_schema_from_samples(&items, TracingOptions::default().allow_null_fields(true))
        .trace_schema_from_type::<Item<U64>>(TracingOptions::default().allow_null_fields(true))
        .serialize(&items)
        .deserialize(&items)
        .check_nulls(&[&[false, false, false]]);
}

#[test]
fn unit() {
    let items = [Item(()), Item(()), Item(()), Item(())];

    Test::new()
        .with_schema(json!([{
            "name": "item",
            "data_type": "Null",
            "nullable": true,
        }]))
        .trace_schema_from_samples(&items, TracingOptions::default().allow_null_fields(true))
        .trace_schema_from_type::<Item<()>>(TracingOptions::default().allow_null_fields(true))
        .serialize(&items)
        .deserialize(&items);
}
