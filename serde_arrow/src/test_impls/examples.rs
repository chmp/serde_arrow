use super::macros::*;

test_example!(
    test_name = benchmark_primitives,
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
