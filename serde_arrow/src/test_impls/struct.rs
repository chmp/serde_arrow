use serde::{Deserialize, Serialize};

use crate::{
    internal::schema::{GenericDataType, GenericField},
    schema::{Strategy, TracingOptions},
    test_impls::utils::Test,
    utils::Item,
};

#[test]
fn r#struct() {
    #[derive(Serialize, Deserialize, Debug, PartialEq)]
    struct S {
        a: u32,
        b: bool,
    }

    type Ty = S;
    let field = GenericField::new("item", GenericDataType::Struct, false)
        .with_child(GenericField::new("a", GenericDataType::U32, false))
        .with_child(GenericField::new("b", GenericDataType::Bool, false));

    let values = [Item(S { a: 1, b: true }), Item(S { a: 2, b: false })];

    let tracing_options = TracingOptions::default();
    Test::new()
        .with_schema(vec![field])
        .trace_schema_from_type::<Item<Ty>>(tracing_options.clone())
        .trace_schema_from_samples(&values, tracing_options.clone())
        .serialize(&values)
        .deserialize(&values);
}

#[test]
fn struct_nested() {
    let field = GenericField::new("item", GenericDataType::Struct, false)
        .with_child(GenericField::new("a", GenericDataType::U32, false))
        .with_child(GenericField::new("b", GenericDataType::Bool, false))
        .with_child(
            GenericField::new("c", GenericDataType::Struct, false)
                .with_child(GenericField::new("d", GenericDataType::I32, false))
                .with_child(GenericField::new("e", GenericDataType::U16, false)),
        );

    type Ty = S;
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
        .with_schema(vec![field])
        .trace_schema_from_type::<Item<Ty>>(tracing_options.clone())
        .trace_schema_from_samples(&values, tracing_options.clone())
        .serialize(&values)
        .deserialize(&values);
}

#[test]
fn struct_nullable_field() {
    let field = GenericField::new("item", GenericDataType::Struct, false)
        .with_child(GenericField::new("a", GenericDataType::U32, true))
        .with_child(GenericField::new("b", GenericDataType::Bool, false));
    type Ty = S;
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
        .with_schema(vec![field])
        .trace_schema_from_type::<Item<Ty>>(tracing_options.clone())
        .trace_schema_from_samples(&values, tracing_options.clone())
        .serialize(&values)
        .deserialize(&values);
}

#[test]
fn nullable_struct() {
    let field = GenericField::new("item", GenericDataType::Struct, true)
        .with_child(GenericField::new("a", GenericDataType::U32, false))
        .with_child(GenericField::new("b", GenericDataType::Bool, false));
    type Ty = Option<S>;
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
        .with_schema(vec![field])
        .trace_schema_from_type::<Item<Ty>>(tracing_options.clone())
        .trace_schema_from_samples(&values, tracing_options.clone())
        .serialize(&values)
        .deserialize(&values);
}

#[test]
fn nullable_nested_struct() {
    let field = GenericField::new("item", GenericDataType::Struct, true)
        .with_child(GenericField::new("a", GenericDataType::U32, false))
        .with_child(
            GenericField::new("b", GenericDataType::Struct, true)
                .with_child(GenericField::new("c", GenericDataType::I16, false))
                .with_child(GenericField::new("d", GenericDataType::F64, false)),
        );
    type Ty = Option<S1>;

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
        .with_schema(vec![field])
        .trace_schema_from_type::<Item<Ty>>(tracing_options.clone())
        .trace_schema_from_samples(&values, tracing_options.clone())
        .serialize(&values)
        .deserialize(&values);
}

#[test]
fn nullable_struct_nullable_fields() {
    let field = GenericField::new("item", GenericDataType::Struct, true)
        .with_child(GenericField::new("a", GenericDataType::U32, true))
        .with_child(GenericField::new("b", GenericDataType::Bool, true));
    type Ty = Option<S>;
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
        .with_schema(vec![field])
        .trace_schema_from_type::<Item<Ty>>(tracing_options.clone())
        .trace_schema_from_samples(&values, tracing_options.clone())
        .serialize(&values)
        .deserialize(&values);
}

#[test]
fn nullable_struct_list_field() {
    let field =
        GenericField::new("item", GenericDataType::Struct, true)
            .with_child(GenericField::new("a", GenericDataType::U32, false))
            .with_child(
                GenericField::new("b", GenericDataType::LargeList, true)
                    .with_child(GenericField::new("element", GenericDataType::Bool, false)),
            );
    type Ty = Option<S>;
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
        .with_schema(vec![field])
        .trace_schema_from_type::<Item<Ty>>(tracing_options.clone())
        .trace_schema_from_samples(&values, tracing_options.clone())
        .serialize(&values)
        .deserialize(&values);
}

#[test]
fn serde_flatten() {
    let field = GenericField::new("item", GenericDataType::Struct, true)
        .with_strategy(Strategy::MapAsStruct)
        .with_child(GenericField::new("a", GenericDataType::I8, false))
        .with_child(GenericField::new("value", GenericDataType::Bool, false));
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
        .with_schema(vec![field])
        .trace_schema_from_samples(&values, tracing_options.clone())
        .serialize(&values)
        .deserialize(&values);
}

#[test]
fn flattened_structures() {
    let field = GenericField::new("item", GenericDataType::Struct, false)
        .with_child(GenericField::new("a", GenericDataType::I64, false))
        .with_child(GenericField::new("b", GenericDataType::F32, false))
        .with_child(GenericField::new("c", GenericDataType::F64, false))
        .with_strategy(Strategy::MapAsStruct);

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
        .with_schema(vec![field])
        .trace_schema_from_samples(&values, tracing_options.clone())
        .serialize(&values)
        .deserialize(&values);
}

#[test]
fn struct_nullable() {
    let tracing_options = TracingOptions::default().allow_null_fields(true);
    let field = GenericField::new("item", GenericDataType::Struct, true)
        .with_child(GenericField::new("a", GenericDataType::Bool, false))
        .with_child(GenericField::new("b", GenericDataType::I64, false))
        .with_child(GenericField::new("c", GenericDataType::Null, true))
        .with_child(GenericField::new("d", GenericDataType::LargeUtf8, false));
    type Ty = Option<Struct>;
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
        .with_schema(vec![field])
        .trace_schema_from_type::<Item<Ty>>(tracing_options.clone())
        .trace_schema_from_samples(&values, tracing_options.clone())
        .serialize(&values)
        .deserialize(&values);
}

#[test]
fn struct_nullable_nested() {
    let tracing_options = TracingOptions::default().allow_null_fields(true);
    let field = GenericField::new("item", GenericDataType::Struct, true).with_child(
        GenericField::new("inner", GenericDataType::Struct, false)
            .with_child(GenericField::new("a", GenericDataType::Bool, false))
            .with_child(GenericField::new("b", GenericDataType::I64, false))
            .with_child(GenericField::new("c", GenericDataType::Null, true))
            .with_child(GenericField::new("d", GenericDataType::LargeUtf8, false)),
    );
    type Ty = Option<Outer>;
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
        .with_schema(vec![field])
        .trace_schema_from_type::<Item<Ty>>(tracing_options.clone())
        .trace_schema_from_samples(&values, tracing_options.clone())
        .serialize(&values)
        .deserialize(&values);
}

#[test]
fn struct_nullable_item() {
    let tracing_options = TracingOptions::default().allow_null_fields(true);
    let field = GenericField::new("item", GenericDataType::Struct, false)
        .with_child(GenericField::new("a", GenericDataType::Bool, true))
        .with_child(GenericField::new("b", GenericDataType::I64, true))
        .with_child(GenericField::new("c", GenericDataType::Null, true))
        .with_child(GenericField::new("d", GenericDataType::LargeUtf8, true));
    type Ty = StructNullable;
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
        .with_schema(vec![field])
        .trace_schema_from_type::<Item<Ty>>(tracing_options.clone())
        .trace_schema_from_samples(&values, tracing_options.clone())
        .serialize(&values)
        .deserialize(&values);
}
