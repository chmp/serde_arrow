use super::macros::*;

test_example!(
    test_name = benchmark_primitives,
    test_bytecode_deserialization = true,
    field = GenericField::new("root", GenericDataType::Struct, false)
        .with_child(GenericField::new("a", GenericDataType::U8, false))
        .with_child(GenericField::new("b", GenericDataType::U16, false))
        .with_child(GenericField::new("c", GenericDataType::U32, false))
        .with_child(GenericField::new("d", GenericDataType::U64, false))
        .with_child(GenericField::new("e", GenericDataType::I8, false))
        .with_child(GenericField::new("f", GenericDataType::I16, false))
        .with_child(GenericField::new("g", GenericDataType::I32, false))
        .with_child(GenericField::new("h", GenericDataType::I64, false))
        .with_child(GenericField::new("i", GenericDataType::F32, false))
        .with_child(GenericField::new("j", GenericDataType::F64, false))
        .with_child(GenericField::new("k", GenericDataType::Bool, false)),
    ty = Item,
    values = [Item::default(), Item::default()],
    nulls = [false, false],
    define = {
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
    },
);

test_example!(
    test_name = benchmark_complex_1,
    test_bytecode_deserialization = true,
    field = GenericField::new("root", GenericDataType::Struct, false)
        .with_child(GenericField::new(
            "string",
            GenericDataType::LargeUtf8,
            false
        ))
        .with_child(
            GenericField::new("points", GenericDataType::LargeList, false).with_child(
                GenericField::new("element", GenericDataType::Struct, false)
                    .with_child(GenericField::new("x", GenericDataType::F32, false))
                    .with_child(GenericField::new("y", GenericDataType::F32, false))
            )
        )
        .with_child(
            GenericField::new("float", GenericDataType::Union, false)
                .with_child(GenericField::new("F32", GenericDataType::F32, false))
                .with_child(GenericField::new("F64", GenericDataType::F64, false))
        ),
    ty = Item,
    values = [
        Item {
            string: "foo".into(),
            points: vec![Point { x: 0.0, y: 0.0 }],
            float: Float::F32(13.0)
        },
        Item {
            string: "foo".into(),
            points: vec![],
            float: Float::F64(21.0)
        },
    ],
    nulls = [false, false],
    define = {
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
    },
);

test_example!(
    test_name = benchmark_complex_2,
    test_bytecode_deserialization = true,
    field = GenericField::new("root", GenericDataType::Struct, false)
        .with_child(GenericField::new(
            "string",
            GenericDataType::LargeUtf8,
            false
        ))
        .with_child(
            GenericField::new("points", GenericDataType::LargeList, false).with_child(
                GenericField::new("element", GenericDataType::Struct, false)
                    .with_child(GenericField::new("x", GenericDataType::F32, false))
                    .with_child(GenericField::new("y", GenericDataType::F32, false))
            )
        )
        .with_child(
            GenericField::new("child", GenericDataType::Struct, false)
                .with_child(GenericField::new("a", GenericDataType::Bool, false))
                .with_child(GenericField::new("b", GenericDataType::F64, false))
                .with_child(GenericField::new("c", GenericDataType::F32, true))
        ),
    ty = Item,
    values = [
        Item {
            string: "foo".into(),
            points: vec![Point { x: 0.0, y: 1.0 }, Point { x: 2.0, y: 3.0 },],
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
    ],
    nulls = [false, false],
    define = {
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
    },
);

test_example!(
    test_name = nested_options,
    test_bytecode_deserialization = true,
    field = GenericField::new("root", GenericDataType::Struct, false)
        .with_child(GenericField::new("a", GenericDataType::U8, false))
        .with_child(GenericField::new("b", GenericDataType::U16, true))
        .with_child(GenericField::new("c", GenericDataType::U32, true)),
    ty = Item,
    values = [
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
    ],
    expected_values = [
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
    ],
    nulls = [false, false, false],
    define = {
        #[derive(Serialize, Deserialize, Debug, PartialEq)]
        struct Item {
            a: u8,
            b: Option<u16>,
            c: Option<Option<u32>>,
        }
    },
);

test_example!(
    test_name = fieldless_unions_in_a_struct,
    test_bytecode_deserialization = true,
    tracing_options = TracingOptions::default().allow_null_fields(true),
    field = GenericField::new("root", GenericDataType::Struct, false)
        .with_child(GenericField::new("foo", GenericDataType::U32, false))
        .with_child(
            GenericField::new("bar", GenericDataType::Union, false)
                .with_child(GenericField::new("A", GenericDataType::Null, true))
                .with_child(GenericField::new("B", GenericDataType::Null, true))
                .with_child(GenericField::new("C", GenericDataType::Null, true))
        )
        .with_child(GenericField::new("baz", GenericDataType::F32, false)),
    ty = S,
    values = [
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
    ],
    nulls = [false, false, false, false],
    define = {
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
    },
);

test_example!(
    // see https://github.com/chmp/serde_arrow/issues/57
    test_name = issue_57,
    test_bytecode_deserialization = true,
    tracing_options = TracingOptions::default().allow_null_fields(true),
    field = GenericField::new("root", GenericDataType::Struct, false)
        .with_child(GenericField::new(
            "filename",
            GenericDataType::LargeUtf8,
            false
        ))
        .with_child(
            GenericField::new("game_type", GenericDataType::Union, false)
                .with_child(
                    GenericField::new("", GenericDataType::Null, true)
                        .with_strategy(Strategy::UnknownVariant)
                )
                .with_child(GenericField::new(
                    "RegularSeason",
                    GenericDataType::Null,
                    true
                ))
        )
        .with_child(
            GenericField::new("account_type", GenericDataType::Union, false)
                .with_child(
                    GenericField::new("", GenericDataType::Null, true)
                        .with_strategy(Strategy::UnknownVariant)
                )
                .with_child(GenericField::new("Deduced", GenericDataType::Null, true))
        )
        .with_child(GenericField::new("file_index", GenericDataType::U64, false)),
    ty = FileInfo,
    values = [FileInfo {
        filename: String::from("test"),
        game_type: GameType::RegularSeason,
        account_type: AccountType::Deduced,
        file_index: 0
    },],
    nulls = [false],
    define = {
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
    },
);

test_roundtrip_arrays!(
    simple_example {
        #[derive(PartialEq, Clone, Debug, Serialize, Deserialize)]
        struct S {
            a: f32,
            b: u32,
        }

        let items = &[
            S{ a: 2.0, b: 4 },
            S{ a: -123.0, b: 9 },
        ];
        let fields = &[
            GenericField::new("a", GenericDataType::F32, false),
            GenericField::new("b", GenericDataType::U32, false),
        ];
    }
    assert_round_trip(fields, items);
);

test_roundtrip_arrays!(
    toplevel_nullables {
        #[derive(PartialEq, Clone, Debug, Serialize, Deserialize)]
        struct S {
            a: Option<f32>,
            b: Option<u32>,
        }

        let items = &[
            S{ a: Some(2.0), b: None },
            S{ a: None, b: Some(9) },
        ];
        let fields = &[
            GenericField::new("a", GenericDataType::F32, true),
            GenericField::new("b", GenericDataType::U32, true),
        ];
    }
    assert_round_trip(fields, items);
);

test_example!(
    test_name = nullable_vec_bool,
    test_bytecode_deserialization = true,
    field = GenericField::new("root", GenericDataType::LargeList, true)
        .with_child(GenericField::new("element", GenericDataType::Bool, false)),
    ty = Option<Vec<bool>>,
    values = [Some(vec![true, false]), None, Some(vec![])],
);

test_example!(
    test_name = nullable_vec_bool_nested,
    test_bytecode_deserialization = true,
    field = GenericField::new("root", GenericDataType::LargeList, true)
        .with_child(GenericField::new("element", GenericDataType::LargeList, false)
            .with_child(GenericField::new("element", GenericDataType::Bool, false))),
    ty = Option<Vec<Vec<bool>>>,
    values = [Some(vec![vec![true], vec![false, false]]), None, Some(vec![vec![]])],
);

test_example!(
    test_name = vec_nullable_bool,
    test_bytecode_deserialization = true,
    field = GenericField::new("root", GenericDataType::LargeList, false)
        .with_child(GenericField::new("element", GenericDataType::Bool, true)),
    ty = Vec<Option<bool>>,
    values = [vec![Some(true), Some(false)], vec![], vec![None, Some(false)]],
);

test_example!(
    test_name = struct_nullable,
    test_bytecode_deserialization = true,
    tracing_options = TracingOptions::default().allow_null_fields(true),
    field = GenericField::new("root",GenericDataType::Struct, true)
        .with_child(GenericField::new("a", GenericDataType::Bool, false))
        .with_child(GenericField::new("b", GenericDataType::I64, false))
        .with_child(GenericField::new("c", GenericDataType::Null, true))
        .with_child(GenericField::new("d", GenericDataType::LargeUtf8, false)),
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

test_example!(
    test_name = struct_nullable_nested,
    test_bytecode_deserialization = true,
    tracing_options = TracingOptions::default().allow_null_fields(true),
    field = GenericField::new("root",GenericDataType::Struct, true)
        .with_child(GenericField::new("inner", GenericDataType::Struct, false)
            .with_child(GenericField::new("a", GenericDataType::Bool, false))
            .with_child(GenericField::new("b", GenericDataType::I64, false))
            .with_child(GenericField::new("c", GenericDataType::Null, true))
            .with_child(GenericField::new("d", GenericDataType::LargeUtf8, false))),
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

test_example!(
    test_name = struct_nullable_item,
    tracing_options = TracingOptions::default().allow_null_fields(true),
    field = GenericField::new("root", GenericDataType::Struct, false)
        .with_child(GenericField::new("a", GenericDataType::Bool, true))
        .with_child(GenericField::new("b", GenericDataType::I64, true))
        .with_child(GenericField::new("c", GenericDataType::Null, true))
        .with_child(GenericField::new("d", GenericDataType::LargeUtf8, true)),
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

// TODO: fix more examples
/*
test_example!(
    test_name = tuple_nullable,
    field = GenericField::new("value", GenericDataType::Struct, true)
        .with_strategy(Strategy::TupleAsStruct)
        .with_child(GenericField::new("0", GenericGenericDataType::Bool, false))
        .with_child(GenericField::new("1", GenericGenericDataType::I64, false)),
    ty = Option<(bool, i64)>,
    values = [
        Some((true, 21)),
        None,
        Some((false, 42)),
    ],
);

test_example!(
    test_name = tuple_nullable_nested,
    field = Field::new("value", DataType::Struct(vec![
        Field::new("0", DataType::Struct(vec![
                Field::new("0", GenericDataType::Bool, false),
                Field::new("1", GenericDataType::I64, false),
            ]), false)
            .with_metadata(strategy_meta(Strategy::TupleAsStruct)),
        Field::new("1", GenericDataType::I64, false),
    ]), true).with_metadata(strategy_meta(Strategy::TupleAsStruct)),
    ty = Option<((bool, i64), i64)>,
    values = [
        Some(((true, 21), 7)),
        None,
        Some(((false, 42), 13)),
    ],
);

test_example!(
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

test_example!(
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

test_example!(
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

test_example!(
    test_name = enums_union,
    tracing_options = TracingOptions::default().allow_null_fields(true),
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

test_example!(
    test_name = hash_maps,
    tracing_options = TracingOptions::new().map_as_struct(false),
    field = Field::new(
        "value",
        DataType::Map(
            Box::new(Field::new(
                "entries",
                DataType::Struct(vec![
                    Field::new("key", GenericDataType::I64, false),
                    Field::new("value", GenericDataType::Bool, false),
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

test_example!(
    test_name = hash_maps_nullable,
    tracing_options = TracingOptions::new().map_as_struct(false),
    field = Field::new(
        "value",
        DataType::Map(
            Box::new(Field::new(
                "entries",
                DataType::Struct(vec![
                    Field::new("key", GenericDataType::I64, false),
                    Field::new("value", GenericDataType::Bool, false),
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

test_example!(
    test_name = hash_maps_nullable_keys,
    tracing_options = TracingOptions::new().map_as_struct(false),
    field = Field::new(
        "value",
        DataType::Map(
            Box::new(Field::new(
                "entries",
                DataType::Struct(vec![
                    Field::new("key", GenericDataType::I64, true),
                    Field::new("value", GenericDataType::Bool, false),
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

test_example!(
    test_name = hash_maps_nullable_values,
    tracing_options = TracingOptions::new().map_as_struct(false),
    field = Field::new(
        "value",
        DataType::Map(
            Box::new(Field::new(
                "entries",
                DataType::Struct(vec![
                    Field::new("key", GenericDataType::I64, false),
                    Field::new("value", GenericDataType::Bool, true),
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

test_example!(
    test_name = btree_maps,
    tracing_options = TracingOptions::new().map_as_struct(false),
    field = Field::new(
        "value",
        DataType::Map(
            Box::new(Field::new(
                "entries",
                DataType::Struct(vec![
                    Field::new("key", GenericDataType::I64, false),
                    Field::new("value", GenericDataType::Bool, false),
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

test_example!(
    test_name = flattened_structures,
    field = Field::new(
        "value",
        DataType::Struct(vec![
            Field::new("a", GenericDataType::I64, false),
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
 */

// TODO: fix these tests
/*

/// Test that dates as RFC 3339 strings are correctly handled
#[test]
fn dtype_date64_naive_str() {
    #[derive(Debug, PartialEq, Serialize, Deserialize)]
    struct Record {
        val: NaiveDateTime,
    }

    let records: &[Record] = &[
        Record {
            val: NaiveDateTime::from_timestamp(12 * 60 * 60 * 24, 0),
        },
        Record {
            val: NaiveDateTime::from_timestamp(9 * 60 * 60 * 24, 0),
        },
    ];

    let mut fields = serialize_into_fields(records, Default::default()).unwrap();

    let val_field = fields.iter_mut().find(|field| field.name == "val").unwrap();
    val_field.data_type = DataType::Date64;
    val_field.metadata = Strategy::NaiveStrAsDate64.into();

    println!("{fields:?}");

    let arrays = serialize_into_arrays(&fields, records).unwrap();

    assert_eq!(arrays.len(), 1);

    let actual = arrays[0]
        .as_any()
        .downcast_ref::<PrimitiveArray<i64>>()
        .unwrap();
    let expected = PrimitiveArray::<i64>::from_slice([12_000 * 60 * 60 * 24, 9_000 * 60 * 60 * 24])
        .to(DataType::Date64);

    assert_eq!(actual, &expected);

    let events = collect_events_from_array(&fields, &arrays).unwrap();
    let expected_events = vec![
        Event::StartSequence,
        Event::StartStruct,
        Event::Str("val").to_static(),
        Event::Str("1970-01-13T00:00:00").to_static(),
        Event::EndStruct,
        Event::StartStruct,
        Event::Str("val").to_static(),
        Event::Str("1970-01-10T00:00:00").to_static(),
        Event::EndStruct,
        Event::EndSequence,
    ];
    assert_eq!(events, expected_events);

    let round_tripped: Vec<Record> = deserialize_from_arrays(&fields, &arrays).unwrap();
    assert_eq!(round_tripped, records);
}

#[test]
fn dtype_date64_str() {
    #[derive(Debug, PartialEq, Serialize, Deserialize)]
    struct Record {
        val: DateTime<Utc>,
    }

    let records: &[Record] = &[
        Record {
            val: Utc.timestamp(12 * 60 * 60 * 24, 0),
        },
        Record {
            val: Utc.timestamp(9 * 60 * 60 * 24, 0),
        },
    ];

    let mut fields = serialize_into_fields(records, Default::default()).unwrap();
    let val_field = fields.iter_mut().find(|field| field.name == "val").unwrap();
    val_field.data_type = DataType::Date64;
    val_field.metadata = Strategy::UtcStrAsDate64.into();

    let arrays = serialize_into_arrays(&fields, records).unwrap();

    assert_eq!(arrays.len(), 1);

    let actual = arrays[0]
        .as_any()
        .downcast_ref::<PrimitiveArray<i64>>()
        .unwrap();
    let expected = PrimitiveArray::<i64>::from_slice([12_000 * 60 * 60 * 24, 9_000 * 60 * 60 * 24])
        .to(DataType::Date64);

    assert_eq!(actual, &expected);

    let events = collect_events_from_array(&fields, &arrays).unwrap();
    let expected_events = vec![
        Event::StartSequence,
        Event::StartStruct,
        Event::Str("val").to_static(),
        Event::Str("1970-01-13T00:00:00Z").to_static(),
        Event::EndStruct,
        Event::StartStruct,
        Event::Str("val").to_static(),
        Event::Str("1970-01-10T00:00:00Z").to_static(),
        Event::EndStruct,
        Event::EndSequence,
    ];
    assert_eq!(events, expected_events);

    let round_tripped: Vec<Record> = deserialize_from_arrays(&fields, &arrays).unwrap();
    assert_eq!(round_tripped, records);
}

#[test]
fn nested_list_structs() {
    #[derive(Debug, Deserialize, Serialize, PartialEq)]
    struct Item {
        a: Vec<Inner>,
    }

    #[derive(Debug, Deserialize, Serialize, PartialEq)]
    struct Inner {
        a: i8,
        b: i32,
    }

    let items = vec![
        Item {
            a: vec![Inner { a: 0, b: 1 }, Inner { a: 2, b: 3 }],
        },
        Item { a: vec![] },
        Item {
            a: vec![Inner { a: 4, b: 5 }],
        },
    ];

    let fields = serialize_into_fields(&items, Default::default()).unwrap();

    let expected_fields = vec![field::large_list(
        "a",
        false,
        field::r#struct(
            "element",
            false,
            [field::int8("a", false), field::int32("b", false)],
        ),
    )];
    assert_eq!(fields, expected_fields);

    let values = serialize_into_arrays(&fields, &items).unwrap();

    let items_from_array: Vec<Item> = deserialize_from_arrays(&fields, &values).unwrap();
    assert_eq!(items_from_array, items);
}

#[test]
fn nested_structs_lists_lists() {
    #[derive(Debug, Deserialize, Serialize, PartialEq)]
    struct Item {
        a: A,
        b: u16,
    }

    #[derive(Debug, Deserialize, Serialize, PartialEq)]
    struct A {
        c: Vec<C>,
    }

    #[derive(Debug, Deserialize, Serialize, PartialEq)]
    struct C {
        d: Vec<u8>,
    }

    let items = vec![
        Item {
            a: A {
                c: vec![C { d: vec![0, 1] }, C { d: vec![2] }],
            },
            b: 3,
        },
        Item {
            a: A { c: vec![] },
            b: 4,
        },
        Item {
            a: A {
                c: vec![C { d: vec![] }],
            },
            b: 5,
        },
    ];

    let fields = serialize_into_fields(&items, Default::default()).unwrap();
    let expected_fields = vec![
        field::r#struct(
            "a",
            false,
            [field::large_list(
                "c",
                false,
                field::r#struct(
                    "element",
                    false,
                    [field::large_list(
                        "d",
                        false,
                        field::uint8("element", false),
                    )],
                ),
            )],
        ),
        field::uint16("b", false),
    ];
    assert_eq!(fields, expected_fields);

    let arrays = serialize_into_arrays(&fields, &items).unwrap();
    let items_from_arrays: Vec<Item> = deserialize_from_arrays(&fields, &arrays).unwrap();

    assert_eq!(items_from_arrays, items);
}

#[test]
fn byte_arrays() {
    #[derive(Debug, PartialEq, Serialize, Deserialize)]
    struct Item {
        a: Vec<u8>,
    }

    let items = vec![
        Item {
            a: b"hello".to_vec(),
        },
        Item {
            a: b"world!".to_vec(),
        },
    ];

    let fields = serialize_into_fields(&items, Default::default()).unwrap();
    let arrays = serialize_into_arrays(&fields, &items).unwrap();

    let items_from_arrays: Vec<Item> = deserialize_from_arrays(&fields, &arrays).unwrap();

    assert_eq!(items_from_arrays, items);
}

#[test]
fn new_type_structs() {
    #[derive(Debug, PartialEq, Serialize, Deserialize)]
    struct Item {
        a: U64,
    }

    #[derive(Debug, PartialEq, Serialize, Deserialize)]
    struct U64(u64);

    let items = vec![Item { a: U64(21) }, Item { a: U64(42) }];

    let fields = serialize_into_fields(&items, Default::default()).unwrap();
    let arrays = serialize_into_arrays(&fields, &items).unwrap();

    let items_from_arrays: Vec<Item> = deserialize_from_arrays(&fields, &arrays).unwrap();

    assert_eq!(items_from_arrays, items);
}

macro_rules! define_wrapper_test {
    ($test_name:ident, $struct_name:ident, $init:expr) => {
        #[test]
        fn $test_name() {
            #[derive(Debug, PartialEq, Serialize, Deserialize)]
            struct $struct_name {
                a: u32,
            }

            let items = $init;

            let fields = serialize_into_fields(&items, Default::default()).unwrap();
            let arrays = serialize_into_arrays(&fields, &items).unwrap();

            let items_from_arrays: Vec<Item> = deserialize_from_arrays(&fields, &arrays).unwrap();

            assert_eq!(items_from_arrays, items);
        }
    };
}

define_wrapper_test!(
    wrapper_outer_vec,
    Item,
    vec![Item { a: 21 }, Item { a: 42 }]
);
define_wrapper_test!(
    wrapper_outer_slice,
    Item,
    [Item { a: 21 }, Item { a: 42 }].as_slice()
);
define_wrapper_test!(wrapper_const_array, Item, [Item { a: 21 }, Item { a: 42 }]);

#[test]
fn wrapper_tuple() {
    #[derive(Debug, PartialEq, Serialize, Deserialize)]
    struct Item {
        a: u32,
    }

    let items = (Item { a: 21 }, Item { a: 42 });

    let fields = serialize_into_fields(&items, Default::default()).unwrap();
    let arrays = serialize_into_arrays(&fields, &items).unwrap();

    let items_from_arrays: Vec<Item> = deserialize_from_arrays(&fields, &arrays).unwrap();

    let items = vec![items.0, items.1];
    assert_eq!(items_from_arrays, items);
}

#[test]
fn test_string_as_large_utf8() {
    #[derive(Debug, PartialEq, Serialize, Deserialize)]
    struct Item {
        a: String,
    }

    let items = vec![
        Item {
            a: String::from("hello"),
        },
        Item {
            a: String::from("world"),
        },
    ];

    let fields = serialize_into_fields(&items, Default::default()).unwrap();
    let expected_fields = vec![Field::new("a", DataType::LargeUtf8, false)];

    assert_eq!(fields, expected_fields);

    let arrays = serialize_into_arrays(&fields, &items).unwrap();
    let items_from_arrays: Vec<Item> = deserialize_from_arrays(&fields, &arrays).unwrap();

    assert_eq!(items_from_arrays, items);
}

#[test]
fn test_string_as_utf8() {
    #[derive(Debug, PartialEq, Serialize, Deserialize)]
    struct Item {
        a: String,
    }

    let items = vec![
        Item {
            a: String::from("hello"),
        },
        Item {
            a: String::from("world"),
        },
    ];

    let fields = vec![Field::new("a", DataType::Utf8, false)];

    let arrays = serialize_into_arrays(&fields, &items).unwrap();
    let items_from_arrays: Vec<Item> = deserialize_from_arrays(&fields, &arrays).unwrap();

    assert_eq!(items_from_arrays, items);
}

#[test]
fn test_unit() {
    #[derive(Debug, PartialEq, Serialize, Deserialize)]
    struct Item {
        a: (),
    }

    let items = vec![Item { a: () }, Item { a: () }];

    let fields =
        serialize_into_fields(&items, TracingOptions::default().allow_null_fields(true)).unwrap();
    let expected_fields = vec![Field::new("a", DataType::Null, true)];

    assert_eq!(fields, expected_fields);

    let arrays = serialize_into_arrays(&fields, &items).unwrap();
    let items_from_arrays: Vec<Item> = deserialize_from_arrays(&fields, &arrays).unwrap();

    assert_eq!(items_from_arrays, items);
}

#[test]
fn test_tuple() {
    #[derive(Debug, PartialEq, Serialize, Deserialize)]
    struct Item {
        a: (u8, u8),
    }

    let items = vec![Item { a: (0, 1) }, Item { a: (2, 3) }];

    let fields = serialize_into_fields(&items, Default::default()).unwrap();
    let arrays = serialize_into_arrays(&fields, &items).unwrap();
    let items_from_arrays: Vec<Item> = deserialize_from_arrays(&fields, &arrays).unwrap();

    assert_eq!(items_from_arrays, items);
}

#[test]
fn test_tuple_struct() {
    #[derive(Debug, PartialEq, Serialize, Deserialize)]
    struct Item {
        a: Inner,
    }

    #[derive(Debug, PartialEq, Serialize, Deserialize)]
    struct Inner(u8, u8);

    let items = vec![Item { a: Inner(0, 1) }, Item { a: Inner(2, 3) }];

    let fields = serialize_into_fields(&items, Default::default()).unwrap();
    let arrays = serialize_into_arrays(&fields, &items).unwrap();
    let items_from_arrays: Vec<Item> = deserialize_from_arrays(&fields, &arrays).unwrap();

    assert_eq!(items_from_arrays, items);
}

#[test]
fn test_struct_with_options() {
    #[derive(Debug, PartialEq, Serialize, Deserialize)]
    struct Item {
        a: Inner,
    }

    #[derive(Debug, PartialEq, Serialize, Deserialize)]
    struct Inner {
        foo: Option<u8>,
        bar: u32,
    }

    let items = vec![
        Item {
            a: Inner { foo: None, bar: 13 },
        },
        Item {
            a: Inner {
                foo: Some(0),
                bar: 21,
            },
        },
        Item {
            a: Inner {
                foo: Some(1),
                bar: 42,
            },
        },
    ];

    let fields = serialize_into_fields(&items, Default::default()).unwrap();
    let arrays = serialize_into_arrays(&fields, &items).unwrap();
    let items_from_arrays: Vec<Item> = deserialize_from_arrays(&fields, &arrays).unwrap();

    assert_eq!(items_from_arrays, items);
}

#[test]
fn test_complex_benchmark_example() {
    use rand::{
        distributions::{Distribution, Standard, Uniform},
        Rng,
    };

    #[derive(Debug, Serialize, Deserialize, PartialEq)]
    struct Item {
        string: String,
        points: Vec<(f32, f32)>,
        float: Float,
    }

    #[derive(Debug, Serialize, Deserialize, PartialEq)]
    enum Float {
        F32(f32),
        F64(f64),
    }

    impl Item {
        fn random<R: Rng + ?Sized>(rng: &mut R) -> Self {
            let n_string = Uniform::new(1, 20).sample(rng);
            let n_points = Uniform::new(1, 20).sample(rng);
            let is_f32: bool = Standard.sample(rng);

            Self {
                string: (0..n_string)
                    .map(|_| -> char { Standard.sample(rng) })
                    .collect(),
                points: (0..n_points)
                    .map(|_| (Standard.sample(rng), Standard.sample(rng)))
                    .collect(),
                float: if is_f32 {
                    Float::F32(Standard.sample(rng))
                } else {
                    Float::F64(Standard.sample(rng))
                },
            }
        }
    }

    let mut rng = rand::thread_rng();
    let items: Vec<Item> = (0..10).map(|_| Item::random(&mut rng)).collect();

    let fields = serialize_into_fields(&items, Default::default()).unwrap();
    let arrays = serialize_into_arrays(&fields, &items).unwrap();

    let round_tripped: Vec<Item> = deserialize_from_arrays(&fields, &arrays).unwrap();

    assert_eq!(items, round_tripped);
}

#[ignore]
#[test]
fn test_maps_with_missing_items() {
    let mut items: Vec<HashMap<String, i32>> = Vec::new();
    let mut item = HashMap::new();
    item.insert(String::from("a"), 0);
    item.insert(String::from("b"), 1);
    items.push(item);

    let mut item = HashMap::new();
    item.insert(String::from("a"), 2);
    item.insert(String::from("c"), 3);
    items.push(item);

    let fields = serialize_into_fields(&items, Default::default()).unwrap();
    let arrays = serialize_into_arrays(&fields, &items).unwrap();
    let actual: Vec<HashMap<String, Option<i32>>> =
        deserialize_from_arrays(&fields, &arrays).unwrap();

    // Note: missing items are serialized as null, therefore the deserialized
    // type must support them
    let mut expected: Vec<HashMap<String, Option<i32>>> = Vec::new();
    let mut item = HashMap::new();
    item.insert(String::from("a"), Some(0));
    item.insert(String::from("b"), Some(1));
    item.insert(String::from("c"), None);
    expected.push(item);

    let mut item = HashMap::new();
    item.insert(String::from("a"), Some(2));
    item.insert(String::from("b"), None);
    item.insert(String::from("c"), Some(3));
    expected.push(item);

    assert_eq!(actual, expected);
}
*/
