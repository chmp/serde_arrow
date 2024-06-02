use serde::{Deserialize, Serialize};

use crate::{
    internal::schema::{GenericDataType, GenericField},
    schema::{Strategy, TracingOptions},
    utils::Item,
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
    let field = GenericField::new("item", GenericDataType::Union, false)
        .with_child(GenericField::new("A", GenericDataType::Null, true))
        .with_child(GenericField::new("B", GenericDataType::Null, true))
        .with_child(GenericField::new("C", GenericDataType::Null, true));

    let values = [Item(U::A), Item(U::B), Item(U::C), Item(U::A)];

    Test::new()
        .with_schema(vec![field])
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
    let field = GenericField::new("item", GenericDataType::Union, false)
        .with_child(GenericField::new("A", GenericDataType::Null, true))
        .with_child(GenericField::new("B", GenericDataType::Null, true))
        .with_child(GenericField::new("C", GenericDataType::Null, true));

    let values = [Item(U::B), Item(U::A), Item(U::C)];

    Test::new()
        .with_schema(vec![field])
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
    let field = GenericField::new("item", GenericDataType::Union, false)
        .with_child(GenericField::new("U32", GenericDataType::U32, false))
        .with_child(GenericField::new("Bool", GenericDataType::Bool, false))
        .with_child(GenericField::new("Str", GenericDataType::LargeUtf8, false));

    let values = [
        Item(U::U32(32)),
        Item(U::Bool(true)),
        Item(U::Str(String::from("hello world"))),
    ];

    Test::new()
        .with_schema(vec![field])
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
    let field =
        GenericField::new("item", GenericDataType::Union, false)
            .with_child(
                GenericField::new("V1", GenericDataType::Struct, false)
                    .with_child(GenericField::new("a", GenericDataType::U32, false))
                    .with_child(GenericField::new("b", GenericDataType::U64, false)),
            )
            .with_child(GenericField::new("Bool", GenericDataType::Bool, false))
            .with_child(
                GenericField::new("S", GenericDataType::Struct, false)
                    .with_child(GenericField::new("s", GenericDataType::LargeUtf8, false)),
            );

    let values = [
        Item(U::V1 { a: 32, b: 13 }),
        Item(U::Bool(true)),
        Item(U::S(S {
            s: String::from("hello world"),
        })),
    ];

    Test::new()
        .with_schema(vec![field])
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

    type Ty = U;

    let tracing_options = TracingOptions::default();
    let field = GenericField::new("item", GenericDataType::Union, false)
        .with_child(GenericField::new("U32", GenericDataType::U32, false))
        .with_child(
            GenericField::new("O", GenericDataType::Union, false)
                .with_child(GenericField::new("Bool", GenericDataType::Bool, false))
                .with_child(GenericField::new("Str", GenericDataType::LargeUtf8, false)),
        );

    let values = [
        Item(U::U32(32)),
        Item(U::O(O::Bool(true))),
        Item(U::O(O::Str(String::from("hello world")))),
        Item(U::U32(16)),
    ];

    Test::new()
        .with_schema(vec![field])
        .trace_schema_from_type::<Item<Ty>>(tracing_options.clone())
        .trace_schema_from_samples(&values, tracing_options.clone())
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
    type Ty = U;

    let tracing_options = TracingOptions::default();
    let field = GenericField::new("item", GenericDataType::Union, false)
        .with_child(GenericField::new("U8", GenericDataType::U8, false))
        .with_child(GenericField::new("U16", GenericDataType::U16, false))
        .with_child(GenericField::new("U32", GenericDataType::U32, false))
        .with_child(GenericField::new("U64", GenericDataType::U64, false));

    let values = [
        Item(U::U32(2)),
        Item(U::U64(3)),
        Item(U::U8(0)),
        Item(U::U16(1)),
    ];

    Test::new()
        .with_schema(vec![field])
        .trace_schema_from_type::<Item<Ty>>(tracing_options.clone())
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
    type Ty = U;

    let tracing_options = TracingOptions::default();
    let field = GenericField::new("item", GenericDataType::Union, false)
        .with_child(
            GenericField::new("A", GenericDataType::Struct, false)
                .with_strategy(Strategy::TupleAsStruct)
                .with_child(GenericField::new("0", GenericDataType::U8, false))
                .with_child(GenericField::new("1", GenericDataType::U32, false)),
        )
        .with_child(
            GenericField::new("B", GenericDataType::Struct, false)
                .with_strategy(Strategy::TupleAsStruct)
                .with_child(GenericField::new("0", GenericDataType::U16, false))
                .with_child(GenericField::new("1", GenericDataType::U64, false)),
        );

    let values = [Item(U::A(2, 3)), Item(U::B(0, 1))];

    Test::new()
        .with_schema(vec![field])
        .trace_schema_from_type::<Item<Ty>>(tracing_options.clone())
        .trace_schema_from_samples(&values, tracing_options.clone())
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
    type Ty = U;

    let tracing_options = TracingOptions::default();
    let field = GenericField::new("item", GenericDataType::Union, false)
        .with_child(
            GenericField::new("A", GenericDataType::Struct, false)
                .with_child(GenericField::new("a", GenericDataType::U8, false))
                .with_child(GenericField::new("b", GenericDataType::U32, false)),
        )
        .with_child(
            GenericField::new("B", GenericDataType::Struct, false)
                .with_child(GenericField::new("c", GenericDataType::U16, false))
                .with_child(GenericField::new("d", GenericDataType::U64, false)),
        );

    let values = [Item(U::A { a: 2, b: 3 }), Item(U::B { c: 0, d: 1 })];

    Test::new()
        .with_schema(vec![field])
        .trace_schema_from_type::<Item<Ty>>(tracing_options.clone())
        .trace_schema_from_samples(&values, tracing_options.clone())
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
    let field = GenericField::new("item", GenericDataType::Union, false)
        .with_child(GenericField::new("A", GenericDataType::Null, true))
        .with_child(GenericField::new("B", GenericDataType::Null, true));

    let values = [Item(U::A), Item(U::B)];

    Test::new()
        .with_schema(vec![field])
        .trace_schema_from_type::<Item<Ty>>(tracing_options.clone())
        .trace_schema_from_samples(&values, tracing_options.clone())
        .serialize(&values)
        .deserialize(&values);
}

macro_rules! test_generic {
    (
        $(#[ignore = $ignore:literal])?
        fn $name:ident() {
            $($stmt:stmt)*
        }
    ) => {
        #[allow(unused)]
        mod $name {
            use crate::{
                schema::{SchemaLike, TracingOptions},
                utils::{Items, Item}
            };
            use crate::internal::schema::{GenericField, GenericDataType};

            mod arrow {
                use super::*;
                use crate::{to_arrow, from_arrow};
                use crate::_impl::arrow::datatypes::Field;

                $(#[ignore = $ignore])?
                #[test]
                fn test() {
                    $($stmt)*
                }
            }
            mod arrow2 {
                use super::*;
                use crate::{to_arrow2 as to_arrow, from_arrow2 as from_arrow};
                use crate::_impl::arrow2::datatypes::Field;

                $(#[ignore = $ignore])?
                #[test]
                fn test() {
                    $($stmt)*
                }
            }
        }
    };
}

test_generic!(
    fn missing_union_variants() {
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
        let fields = Vec::<Field>::from_samples(&Items(&[U::A, U::C]), tracing_options).unwrap();

        // NOTE: variant B was never encountered during tracing
        let res = to_arrow(&fields, &Items(&[U::A, U::B, U::C]));
        assert_error(&res, "Serialization failed: an unknown variant");
    }
);
