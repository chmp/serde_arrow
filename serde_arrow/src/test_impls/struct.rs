use super::macros::*;

test_example!(
    test_name = struct_,
    field = GenericField::new("item", GenericDataType::Struct, false)
        .with_child(GenericField::new("a", GenericDataType::U32, false))
        .with_child(GenericField::new("b", GenericDataType::Bool, false)),
    ty = S,
    values = [S { a: 1, b: true }, S { a: 2, b: false }],
    nulls = [false, false],
    define = {
        #[derive(Serialize, Deserialize, Debug, PartialEq)]
        struct S {
            a: u32,
            b: bool,
        }
    },
);

test_example!(
    test_name = struct_nested,
    field = GenericField::new("item", GenericDataType::Struct, false)
        .with_child(GenericField::new("a", GenericDataType::U32, false))
        .with_child(GenericField::new("b", GenericDataType::Bool, false))
        .with_child(
            GenericField::new("c", GenericDataType::Struct, false)
                .with_child(GenericField::new("d", GenericDataType::I32, false))
                .with_child(GenericField::new("e", GenericDataType::U16, false))
        ),
    ty = S,
    values = [S::default(), S::default()],
    nulls = [false, false],
    define = {
        #[derive(Default, Serialize, Deserialize, Debug, PartialEq)]
        struct S {
            a: u32,
            b: bool,
            c: T,
        }

        #[derive(Default, Serialize, Deserialize, Debug, PartialEq)]
        struct T {
            d: i32,
            e: u16,
        }
    },
);

test_example!(
    test_name = struct_nullable_field,
    field = GenericField::new("item", GenericDataType::Struct, false)
        .with_child(GenericField::new("a", GenericDataType::U32, true))
        .with_child(GenericField::new("b", GenericDataType::Bool, false)),
    ty = S,
    values = [
        S {
            a: Some(1),
            b: true
        },
        S {
            a: Some(2),
            b: false
        }
    ],
    nulls = [false, false],
    define = {
        #[derive(Serialize, Deserialize, Debug, PartialEq)]
        struct S {
            a: Option<u32>,
            b: bool,
        }
    },
);

test_example!(
    test_name = nullable_struct,

    field = GenericField::new("item", GenericDataType::Struct, true)
        .with_child(GenericField::new("a", GenericDataType::U32, false))
        .with_child(GenericField::new("b", GenericDataType::Bool, false)),
    ty = Option<S>,
    values = [Some(S { a: 1, b: true }), None, Some(S { a: 3, b: false })],
    nulls = [false, true, false],
    define = {
        #[derive(Serialize, Deserialize, Debug, PartialEq)]
        struct S {
            a: u32,
            b: bool,
        }
    },
);

test_example!(
    test_name = nullable_nested_struct,

    field = GenericField::new("item", GenericDataType::Struct, true)
        .with_child(GenericField::new("a", GenericDataType::U32, false))
        .with_child(GenericField::new("b", GenericDataType::Struct, true)
            .with_child(GenericField::new("c", GenericDataType::I16, false))
            .with_child(GenericField::new("d", GenericDataType::F64, false))),
    ty = Option<S1>,
    values = [Some(S1 { a: 1, b: None }), None, Some(S1 { a: 3, b: Some(S2{ c: -7, d: 42.0}) })],
    nulls = [false, true, false],
    define = {
        #[derive(Serialize, Deserialize, Debug, PartialEq)]
        struct S1 {
            a: u32,
            b: Option<S2>,
        }

        #[derive(Serialize, Deserialize, Debug, PartialEq)]
        struct S2 {
            c: i16,
            d: f64,
        }
    },
);

test_example!(
    test_name = nullable_struct_nullable_fields,

    field = GenericField::new("item", GenericDataType::Struct, true)
        .with_child(GenericField::new("a", GenericDataType::U32, true))
        .with_child(GenericField::new("b", GenericDataType::Bool, true)),
    ty = Option<S>,
    values = [
        Some(S { a: Some(1), b: Some(true) }),
        Some(S { a: Some(1), b: None }),
        Some(S { a: None, b: Some(true) }),
        Some(S { a: None, b: None }),
        None,
    ],
    nulls = [false, false, false, false, true],
    define = {
        #[derive(Serialize, Deserialize, Debug, PartialEq)]
        struct S {
            a: Option<u32>,
            b: Option<bool>,
        }
    },
);

// arrow2 panics with: OutOfSpec("A StructArray must contain at least one field")
// test_example!(
//     test_name = empt_struct,
//     field = GenericField::new("item", GenericDataType::Struct, false),
//     ty = S,
//     values = [S {}, S {}, S {}],
//     nulls = [false, false, false],
//     define = {
//         #[derive(Serialize)]
//         struct S {}
//     },
// );

test_example!(
    test_name = nullable_struct_list_field,

    field = GenericField::new("item", GenericDataType::Struct, true)
        .with_child(GenericField::new("a", GenericDataType::U32, false))
        .with_child(GenericField::new("b", GenericDataType::LargeList, true)
            .with_child(GenericField::new("element", GenericDataType::Bool, false))),
    ty = Option<S>,
    values = [
        Some(S { a: 1, b: None }),
        Some(S { a: 3, b: Some(vec![]) }),
        None,
        Some(S { a: 3, b: Some(vec![true, false, true]) }),
    ],
    nulls = [false, false, true, false],
    define = {
        #[derive(Serialize, Deserialize, Debug, PartialEq)]
        struct S {
            a: u32,
            b: Option<Vec<bool>>,
        }
    },
);

test_example!(
    // #[ignore = "error during serialization"]
    test_name = serde_flatten,
    field = GenericField::new("item", GenericDataType::Struct, false)
        .with_strategy(Strategy::MapAsStruct)
        .with_child(GenericField::new("a", GenericDataType::I8, false))
        .with_child(GenericField::new("value", GenericDataType::Bool, false)),
    ty = Item,
    values = [Item {
        a: 0,
        b: Inner { value: true },
    },],
    nulls = [false],
    define = {
        #[derive(Debug, Serialize, Deserialize, PartialEq)]
        struct Item {
            a: i8,
            #[serde(flatten)]
            b: Inner,
        }

        #[derive(Debug, Serialize, Deserialize, PartialEq)]
        struct Inner {
            value: bool,
        }
    },
);

test_example!(
    test_name = flattened_structures,
    field = GenericField::new("item", GenericDataType::Struct, false)
        .with_child(GenericField::new("a", GenericDataType::I64, false))
        .with_child(GenericField::new("b", GenericDataType::F32, false))
        .with_child(GenericField::new("c", GenericDataType::F64, false))
        .with_strategy(Strategy::MapAsStruct),
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

test_example!(
    test_name = struct_nullable,

    tracing_options = TracingOptions::default().allow_null_fields(true),
    field = GenericField::new("item",GenericDataType::Struct, true)
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

    tracing_options = TracingOptions::default().allow_null_fields(true),
    field = GenericField::new("item",GenericDataType::Struct, true)
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
    field = GenericField::new("item", GenericDataType::Struct, false)
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

test_events!(
    test_name = out_of_order_fields,
    fields = [
        GenericField::new("foo", GenericDataType::U32, false),
        GenericField::new("bar", GenericDataType::U8, false),
    ],
    events = [
        Event::StartSequence,
        Event::Item,
        Event::StartStruct,
        Event::Str("foo"),
        Event::U32(0),
        Event::Str("bar"),
        Event::U8(1),
        Event::EndStruct,
        Event::Item,
        Event::StartStruct,
        Event::Str("bar"),
        Event::U8(2),
        Event::Str("foo"),
        Event::U32(3),
        Event::EndStruct,
        Event::EndSequence,
    ],
);
