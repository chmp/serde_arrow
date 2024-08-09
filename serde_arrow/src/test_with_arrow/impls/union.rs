use serde::{Deserialize, Serialize};
use serde_json::json;

use crate::internal::{
    schema::{SchemaLike, TracingOptions},
    utils::{Item, Items},
};

use super::utils::Test;

#[test]
fn fieldless_unions() {
    #[derive(Serialize, Deserialize, Debug, PartialEq)]
    enum U {
        A,
        B,
        C,
    }

    type Ty = U;

    let tracing_options = TracingOptions::default().allow_null_fields(true);
    let values = [Item(U::A), Item(U::B), Item(U::C), Item(U::A)];

    Test::new()
        .with_schema(json!([
            {
                "name": "item",
                "data_type": "Union",
                "children": [
                    {"name": "A", "data_type": "Null"},
                    {"name": "B", "data_type": "Null"},
                    {"name": "C", "data_type": "Null"},
                ],
            },
        ]))
        .trace_schema_from_type::<Item<Ty>>(tracing_options.clone())
        .trace_schema_from_samples(&values, tracing_options.clone())
        .serialize(&values)
        .deserialize(&values);
}

#[test]
fn fieldless_union_out_of_order() {
    #[derive(Serialize, Deserialize, Debug, PartialEq)]
    enum U {
        A,
        B,
        C,
    }

    type Ty = U;

    let tracing_options = TracingOptions::default().allow_null_fields(true);
    let values = [Item(U::B), Item(U::A), Item(U::C)];

    Test::new()
        .with_schema(json!([
            {
                "name": "item",
                "data_type": "Union",
                "children": [
                    {"name": "A", "data_type": "Null", "nullable": true},
                    {"name": "B", "data_type": "Null", "nullable": true},
                    {"name": "C", "data_type": "Null", "nullable": true},
                ],
            },
        ]))
        .trace_schema_from_type::<Item<Ty>>(tracing_options.clone())
        .trace_schema_from_samples(&values, tracing_options.clone())
        .serialize(&values)
        .deserialize(&values);
}

#[test]
fn union_simple() {
    #[derive(Serialize, Deserialize, Debug, PartialEq)]
    enum U {
        U32(u32),
        Bool(bool),
        Str(String),
    }

    type Ty = U;

    let tracing_options = TracingOptions::default();
    let values = [
        Item(U::U32(32)),
        Item(U::Bool(true)),
        Item(U::Str(String::from("hello world"))),
    ];

    Test::new()
        .with_schema(json!([
            {
                "name": "item",
                "data_type": "Union",
                "children": [
                    {"name": "U32", "data_type": "U32"},
                    {"name": "Bool", "data_type": "Bool"},
                    {"name": "Str", "data_type": "LargeUtf8"},
                ],
            },
        ]))
        .trace_schema_from_type::<Item<Ty>>(tracing_options.clone())
        .trace_schema_from_samples(&values, tracing_options.clone())
        .serialize(&values)
        .deserialize(&values);
}

#[test]
fn union_mixed() {
    #[derive(Serialize, Deserialize, Debug, PartialEq)]
    enum U {
        V1 { a: u32, b: u64 },
        Bool(bool),
        S(S),
    }

    #[derive(Serialize, Deserialize, Debug, PartialEq)]
    struct S {
        s: String,
    }
    type Ty = U;

    let tracing_options = TracingOptions::default();
    let values = [
        Item(U::V1 { a: 32, b: 13 }),
        Item(U::Bool(true)),
        Item(U::S(S {
            s: String::from("hello world"),
        })),
    ];

    Test::new()
        .with_schema(json!([
            {
                "name": "item",
                "data_type": "Union",
                "children": [
                    {
                        "name": "V1",
                        "data_type": "Struct",
                        "children": [
                            {"name": "a", "data_type": "U32"},
                            {"name": "b", "data_type": "U64"},
                        ],
                    },
                    {"name": "Bool", "data_type": "Bool"},
                    {
                        "name": "S",
                        "data_type": "Struct",
                        "children": [
                            {"name": "s", "data_type": "LargeUtf8"},
                        ]
                    },
                ],
            },
        ]))
        .trace_schema_from_type::<Item<Ty>>(tracing_options.clone())
        .trace_schema_from_samples(&values, tracing_options.clone())
        .serialize(&values)
        .deserialize(&values);
}

#[test]
fn union_nested() {
    #[derive(Serialize, Deserialize, Debug, PartialEq)]
    enum U {
        U32(u32),
        O(O),
    }

    #[derive(Serialize, Deserialize, Debug, PartialEq)]
    enum O {
        Bool(bool),
        Str(String),
    }

    let values = [
        Item(U::U32(32)),
        Item(U::O(O::Bool(true))),
        Item(U::O(O::Str(String::from("hello world")))),
        Item(U::U32(16)),
    ];

    Test::new()
        .with_schema(json!([
            {
                "name": "item",
                "data_type": "Union",
                "children": [
                    {"name": "U32", "data_type": "U32"},
                    {
                        "name": "O",
                        "data_type": "Union",
                        "children": [
                            {"name": "Bool", "data_type": "Bool"},
                            {"name": "Str", "data_type": "LargeUtf8"},
                        ],
                    },
                ],
            },
        ]))
        .trace_schema_from_type::<Item<U>>(TracingOptions::default())
        .trace_schema_from_samples(&values, TracingOptions::default())
        .serialize(&values)
        .deserialize(&values);
}

#[test]
fn enums() {
    #[derive(Debug, PartialEq, Deserialize, Serialize)]
    enum U {
        U8(u8),
        U16(u16),
        U32(u32),
        U64(u64),
    }

    let tracing_options = TracingOptions::default();
    let values = [
        Item(U::U32(2)),
        Item(U::U64(3)),
        Item(U::U8(0)),
        Item(U::U16(1)),
    ];

    Test::new()
        .with_schema(json!([
            {
                "name": "item",
                "data_type": "Union",
                "children": [
                    {"name": "U8", "data_type": "U8"},
                    {"name": "U16", "data_type": "U16"},
                    {"name": "U32", "data_type": "U32"},
                    {"name": "U64", "data_type": "U64"}
                ],
            },
        ]))
        .trace_schema_from_type::<Item<U>>(tracing_options.clone())
        .trace_schema_from_samples(&values, tracing_options.clone())
        .serialize(&values)
        .deserialize(&values);
}

#[test]
fn enums_tuple() {
    #[derive(Debug, PartialEq, Deserialize, Serialize)]
    enum U {
        A(u8, u32),
        B(u16, u64),
    }

    let values = [Item(U::A(2, 3)), Item(U::B(0, 1))];

    Test::new()
        .with_schema(json!([
            {
                "name": "item",
                "data_type": "Union",
                "children": [
                    {
                        "name": "A",
                        "data_type": "Struct",
                        "strategy": "TupleAsStruct",
                        "children": [
                            {"name": "0", "data_type": "U8"},
                            {"name": "1", "data_type": "U32"},
                        ],
                    },
                    {
                        "name": "B",
                        "data_type": "Struct",
                        "strategy": "TupleAsStruct",
                        "children": [
                            {"name": "0", "data_type": "U16"},
                            {"name": "1", "data_type": "U64"},
                        ],
                    },
                ],
            },
        ]))
        .trace_schema_from_type::<Item<U>>(TracingOptions::default())
        .trace_schema_from_samples(&values, TracingOptions::default())
        .serialize(&values)
        .deserialize(&values);
}

#[test]
fn enums_struct() {
    #[derive(Debug, PartialEq, Deserialize, Serialize)]
    enum U {
        A { a: u8, b: u32 },
        B { c: u16, d: u64 },
    }
    let values = [Item(U::A { a: 2, b: 3 }), Item(U::B { c: 0, d: 1 })];

    Test::new()
        .with_schema(json!([
            {
                "name": "item",
                "data_type": "Union",
                "children": [
                    {
                        "name": "A",
                        "data_type": "Struct",
                        "children": [
                            {"name": "a", "data_type": "U8"},
                            {"name": "b", "data_type": "U32"},
                        ],
                    },
                    {
                        "name": "B",
                        "data_type": "Struct",
                        "children": [
                            {"name": "c", "data_type": "U16"},
                            {"name": "d", "data_type": "U64"},
                        ],
                    },
                ],
            },
        ]))
        .trace_schema_from_type::<Item<U>>(TracingOptions::default())
        .trace_schema_from_samples(&values, TracingOptions::default())
        .serialize(&values)
        .deserialize(&values);
}

#[test]
fn enums_union() {
    #[derive(Debug, PartialEq, Deserialize, Serialize)]
    enum U {
        A,
        B,
    }
    type Ty = U;

    let tracing_options = TracingOptions::default().allow_null_fields(true);
    let values = [Item(U::A), Item(U::B)];

    Test::new()
        .with_schema(json!([
            {
                "name": "item",
                "data_type": "Union",
                "children": [
                    {"name": "A", "data_type": "Null", "nullable": true},
                    {"name": "B", "data_type": "Null", "nullable": true},
                ],
            },
        ]))
        .trace_schema_from_type::<Item<Ty>>(tracing_options.clone())
        .trace_schema_from_samples(&values, tracing_options.clone())
        .serialize(&values)
        .deserialize(&values);
}

#[test]
fn missing_union_variants() {
    use crate::_impl::arrow::datatypes::FieldRef;

    use crate::internal::testing::assert_error;
    use crate::schema::TracingOptions;
    use serde::{Deserialize, Serialize};

    #[derive(Serialize, Deserialize, Debug, PartialEq)]
    enum U {
        A,
        B,
        C,
    }

    let tracing_options = TracingOptions::default().allow_null_fields(true);
    let fields = Vec::<FieldRef>::from_samples(&Items(&[U::A, U::C]), tracing_options).unwrap();

    // NOTE: variant B was never encountered during tracing
    let res = crate::to_arrow(&fields, &Items(&[U::A, U::B, U::C]));
    assert_error(&res, "Serialization failed: an unknown variant");
}

#[test]
fn fieldless_unions_as_dictionary() {
    #[derive(Serialize, Deserialize, Debug, PartialEq)]
    enum U {
        A,
        B,
        C,
    }

    let tracing_options = TracingOptions::default().enums_without_data_as_strings(true);
    let values = [Item(U::A), Item(U::B), Item(U::C), Item(U::A)];

    Test::new()
        .with_schema(json!([{
            "name": "item",
            "data_type": "Dictionary",
            "children": [
                {"name": "key", "data_type": "U32"},
                {"name": "value", "data_type": "LargeUtf8"},
            ]
        }]))
        .trace_schema_from_type::<Item<U>>(tracing_options.clone())
        .trace_schema_from_samples(&values, tracing_options.clone())
        .serialize(&values)
        .deserialize(&values);
}

#[test]
fn fieldless_unions_as_utf8() {
    #[derive(Serialize, Deserialize, Debug, PartialEq)]
    enum U {
        A,
        B,
        C,
    }

    let values = [Item(U::A), Item(U::B), Item(U::C), Item(U::A)];

    Test::new()
        .with_schema(json!([{"name": "item", "data_type": "Utf8"}]))
        .serialize(&values)
        .deserialize(&values);
}

#[test]
fn fieldless_unions_as_large_utf8() {
    #[derive(Serialize, Deserialize, Debug, PartialEq)]
    enum U {
        A,
        B,
        C,
    }

    let values = [Item(U::A), Item(U::B), Item(U::C), Item(U::A)];

    Test::new()
        .with_schema(json!([{"name": "item", "data_type": "LargeUtf8"}]))
        .serialize(&values)
        .deserialize(&values);
}
