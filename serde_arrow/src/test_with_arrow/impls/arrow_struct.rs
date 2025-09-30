use serde::{Deserialize, Serialize};
use serde_json::json;

use crate::internal::{schema::TracingOptions, utils::Item};

use super::utils::Test;

#[test]
fn r#struct() {
    #[derive(Serialize, Deserialize, Debug, PartialEq)]
    struct S {
        a: u32,
        b: bool,
    }
    let values = [Item(S { a: 1, b: true }), Item(S { a: 2, b: false })];

    let tracing_options = TracingOptions::default();
    Test::new()
        .with_schema(json!([
            {
                "name": "item",
                "data_type": "Struct",
                "children": [
                    {"name": "a", "data_type": "U32"},
                    {"name": "b", "data_type": "Bool"},
                ],
            }
        ]))
        .trace_schema_from_type::<Item<S>>(tracing_options.clone())
        .trace_schema_from_samples(&values, tracing_options.clone())
        .serialize(&values)
        .deserialize(&values);
}

#[test]
fn struct_nested() {
    let values = [Item(S::default()), Item(S::default())];

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
    let tracing_options = TracingOptions::default();
    Test::new()
        .with_schema(json!([
            {
                "name": "item",
                "data_type": "Struct",
                "children": [
                    {"name": "a", "data_type": "U32"},
                    {"name": "b", "data_type": "Bool"},
                    {
                        "name": "c",
                        "data_type": "Struct",
                        "children": [
                            {"name": "d", "data_type": "I32"},
                            {"name": "e", "data_type": "U16"},
                        ],
                    }
                ],
            }
        ]))
        .trace_schema_from_type::<Item<S>>(tracing_options.clone())
        .trace_schema_from_samples(&values, tracing_options.clone())
        .serialize(&values)
        .deserialize(&values);
}

#[test]
fn struct_nullable_field() {
    let values = [
        Item(S {
            a: Some(1),
            b: true,
        }),
        Item(S {
            a: Some(2),
            b: false,
        }),
    ];

    #[derive(Serialize, Deserialize, Debug, PartialEq)]
    struct S {
        a: Option<u32>,
        b: bool,
    }

    let tracing_options = TracingOptions::default();
    Test::new()
        .with_schema(json!([
            {
                "name": "item",
                "data_type": "Struct",
                "children": [
                    {"name": "a", "data_type": "U32", "nullable": true},
                    {"name": "b", "data_type": "Bool"},
                ],
            }
        ]))
        .trace_schema_from_type::<Item<S>>(tracing_options.clone())
        .trace_schema_from_samples(&values, tracing_options.clone())
        .serialize(&values)
        .deserialize(&values);
}

#[test]
fn nullable_struct() {
    let values = [
        Item(Some(S { a: 1, b: true })),
        Item(None),
        Item(Some(S { a: 3, b: false })),
    ];
    #[derive(Serialize, Deserialize, Debug, PartialEq)]
    struct S {
        a: u32,
        b: bool,
    }

    let tracing_options = TracingOptions::default();
    Test::new()
        .with_schema(json!([
            {
                "name": "item",
                "data_type": "Struct",
                "nullable": true,
                "children": [
                    {"name": "a", "data_type": "U32"},
                    {"name": "b", "data_type": "Bool"},
                ],
            }
        ]))
        .trace_schema_from_type::<Item<Option<S>>>(tracing_options.clone())
        .trace_schema_from_samples(&values, tracing_options.clone())
        .serialize(&values)
        .deserialize(&values);
}

#[test]
fn nullable_nested_struct() {
    let values = [
        Item(Some(S1 { a: 1, b: None })),
        Item(None),
        Item(Some(S1 {
            a: 3,
            b: Some(S2 { c: -7, d: 42.0 }),
        })),
    ];

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

    let tracing_options = TracingOptions::default();
    Test::new()
        .with_schema(json!([
            {
                "name": "item",
                "data_type": "Struct",
                "nullable": true,
                "children": [
                    {"name": "a", "data_type": "U32"},
                    {
                        "name": "b",
                        "data_type": "Struct",
                        "nullable": true,
                        "children": [
                            {"name": "c", "data_type": "I16"},
                            {"name": "d", "data_type": "F64"},
                        ]
                    },
                ],
            }
        ]))
        .trace_schema_from_type::<Item<Option<S1>>>(tracing_options.clone())
        .trace_schema_from_samples(&values, tracing_options.clone())
        .serialize(&values)
        .deserialize(&values);
}

#[test]
fn nullable_struct_nullable_fields() {
    let values = [
        Item(Some(S {
            a: Some(1),
            b: Some(true),
        })),
        Item(Some(S {
            a: Some(1),
            b: None,
        })),
        Item(Some(S {
            a: None,
            b: Some(true),
        })),
        Item(Some(S { a: None, b: None })),
        Item(None),
    ];
    #[derive(Serialize, Deserialize, Debug, PartialEq)]
    struct S {
        a: Option<u32>,
        b: Option<bool>,
    }
    let tracing_options = TracingOptions::default();
    Test::new()
        .with_schema(json!([
            {
                "name": "item",
                "data_type": "Struct",
                "nullable": true,
                "children": [
                    {"name": "a", "data_type": "U32", "nullable": true},
                    {"name": "b", "data_type": "Bool", "nullable": true},
                ],
            }
        ]))
        .trace_schema_from_type::<Item<Option<S>>>(tracing_options.clone())
        .trace_schema_from_samples(&values, tracing_options.clone())
        .serialize(&values)
        .deserialize(&values);
}

#[test]
fn nullable_struct_list_field() {
    let values = [
        Item(Some(S { a: 1, b: None })),
        Item(Some(S {
            a: 3,
            b: Some(vec![]),
        })),
        Item(None),
        Item(Some(S {
            a: 3,
            b: Some(vec![true, false, true]),
        })),
    ];

    #[derive(Serialize, Deserialize, Debug, PartialEq)]
    struct S {
        a: u32,
        b: Option<Vec<bool>>,
    }
    let tracing_options = TracingOptions::default();
    Test::new()
        .with_schema(json!([
            {
                "name": "item",
                "data_type": "Struct",
                "nullable": true,
                "children": [
                    {"name": "a", "data_type": "U32"},
                    {
                        "name": "b",
                        "data_type": "LargeList",
                        "nullable": true,
                        "children": [
                            {"name": "element", "data_type": "Bool"},
                        ],
                    },
                ],
            }
        ]))
        .trace_schema_from_type::<Item<Option<S>>>(tracing_options.clone())
        .trace_schema_from_samples(&values, tracing_options.clone())
        .serialize(&values)
        .deserialize(&values);
}

#[test]
fn serde_flatten() {
    let values = [Item(Some(LocalItem {
        a: 0,
        b: Inner { value: true },
    }))];

    #[derive(Debug, Serialize, Deserialize, PartialEq)]
    struct LocalItem {
        a: i8,
        #[serde(flatten)]
        b: Inner,
    }

    #[derive(Debug, Serialize, Deserialize, PartialEq)]
    struct Inner {
        value: bool,
    }
    let tracing_options = TracingOptions::default().map_as_struct(true);
    Test::new()
        .with_schema(json!([
            {
                "name": "item",
                "data_type": "Struct",
                "nullable": true,
                "strategy": "MapAsStruct",
                "children": [
                    {"name": "a", "data_type": "I8"},
                    {"name": "value", "data_type": "Bool"},
                ],
            },
        ]))
        .trace_schema_from_samples(&values, tracing_options.clone())
        .serialize(&values)
        .deserialize(&values);
}

#[test]
fn flattened_structures() {
    let values = [
        Item(Outer {
            a: 0,
            inner: Inner { b: 1.0, c: 2.0 },
        }),
        Item(Outer {
            a: 3,
            inner: Inner { b: 4.0, c: 5.0 },
        }),
        Item(Outer {
            a: 6,
            inner: Inner { b: 7.0, c: 8.0 },
        }),
    ];
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
    let tracing_options = TracingOptions::default();
    Test::new()
        .with_schema(json!([
            {
                "name": "item",
                "data_type": "Struct",
                "strategy": "MapAsStruct",
                "children": [
                    {"name": "a", "data_type": "I64"},
                    {"name": "b", "data_type": "F32"},
                    {"name": "c", "data_type": "F64"},
                ],
            }
        ]))
        .trace_schema_from_samples(&values, tracing_options.clone())
        .serialize(&values)
        .deserialize(&values);
}

#[test]
fn struct_nullable() {
    #[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
    struct Struct {
        a: bool,
        b: i64,
        c: (),
        d: String,
    }

    let tracing_options = TracingOptions::default().allow_null_fields(true);
    let values = [
        Item(Some(Struct {
            a: true,
            b: 42,
            c: (),
            d: String::from("hello"),
        })),
        Item(None),
        Item(Some(Struct {
            a: false,
            b: 13,
            c: (),
            d: String::from("world"),
        })),
    ];

    Test::new()
        .with_schema(json!([
            {
                "name": "item",
                "data_type": "Struct",
                "nullable": true,
                "children": [
                    {"name": "a", "data_type": "Bool"},
                    {"name": "b", "data_type": "I64"},
                    {"name": "c", "data_type": "Null"},
                    {"name": "d", "data_type": "LargeUtf8"},
                ],
            }
        ]))
        .trace_schema_from_type::<Item<Option<Struct>>>(tracing_options.clone())
        .trace_schema_from_samples(&values, tracing_options.clone())
        .serialize(&values)
        .deserialize(&values);
}

#[test]
fn struct_nullable_nested() {
    let tracing_options = TracingOptions::default().allow_null_fields(true);
    let values = [
        Item(Some(Outer {
            inner: Struct {
                a: true,
                b: 42,
                c: (),
                d: String::from("hello"),
            },
        })),
        Item(None),
        Item(Some(Outer {
            inner: Struct {
                a: false,
                b: 13,
                c: (),
                d: String::from("world"),
            },
        })),
    ];
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

    Test::new()
        .with_schema(json!([
            {
                "name": "item",
                "data_type": "Struct",
                "nullable": true,
                "children": [
                    {
                        "name": "inner",
                        "data_type": "Struct",
                        "children": [
                            {"name": "a", "data_type": "Bool"},
                            {"name": "b", "data_type": "I64"},
                            {"name": "c", "data_type": "Null"},
                            {"name": "d", "data_type": "LargeUtf8"},
                        ]
                    },
                ],
            }
        ]))
        .trace_schema_from_type::<Item<Option<Outer>>>(tracing_options.clone())
        .trace_schema_from_samples(&values, tracing_options.clone())
        .serialize(&values)
        .deserialize(&values);
}

#[test]
fn struct_nullable_item() {
    let tracing_options = TracingOptions::default().allow_null_fields(true);
    let values = [
        Item(StructNullable {
            a: None,
            b: None,
            c: None,
            d: Some(String::from("hello")),
        }),
        Item(StructNullable {
            a: Some(true),
            b: Some(42),
            c: None,
            d: None,
        }),
    ];

    #[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
    struct StructNullable {
        a: Option<bool>,
        b: Option<i64>,
        c: Option<()>,
        d: Option<String>,
    }

    Test::new()
        .with_schema(json!([
            {
                "name": "item",
                "data_type": "Struct",
                "children": [
                    {"name": "a", "data_type": "Bool", "nullable": true},
                    {"name": "b", "data_type": "I64", "nullable": true},
                    {"name": "c", "data_type": "Null", "nullable": true},
                    {"name": "d", "data_type": "LargeUtf8", "nullable": true},
                ],
            }
        ]))
        .trace_schema_from_type::<Item<StructNullable>>(tracing_options.clone())
        .trace_schema_from_samples(&values, tracing_options.clone())
        .serialize(&values)
        .deserialize(&values);
}
