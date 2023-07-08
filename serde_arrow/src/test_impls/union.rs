use super::macros::{test_error, test_example};

test_example!(
    test_name = fieldless_unions,
    test_bytecode_deserialization = true,
    tracing_options = TracingOptions::default().allow_null_fields(true),
    field = GenericField::new("root", GenericDataType::Union, false)
        .with_child(GenericField::new("A", GenericDataType::Null, true))
        .with_child(GenericField::new("B", GenericDataType::Null, true))
        .with_child(GenericField::new("C", GenericDataType::Null, true)),
    ty = U,
    values = [U::A, U::B, U::C, U::A,],
    nulls = [false, false, false, false],
    define = {
        #[derive(Serialize, Deserialize, Debug, PartialEq)]
        enum U {
            A,
            B,
            C,
        }
    },
);

test_example!(
    test_name = fieldless_union_out_of_order,
    test_bytecode_deserialization = true,
    tracing_options = TracingOptions::default().allow_null_fields(true),
    field = GenericField::new("root", GenericDataType::Union, false)
        .with_child(GenericField::new("A", GenericDataType::Null, true))
        .with_child(GenericField::new("B", GenericDataType::Null, true))
        .with_child(GenericField::new("C", GenericDataType::Null, true)),
    ty = U,
    values = [U::B, U::A, U::C],
    nulls = [false, false, false],
    define = {
        #[derive(Serialize, Deserialize, Debug, PartialEq)]
        enum U {
            A,
            B,
            C,
        }
    },
);

test_example!(
    test_name = union_simple,
    test_bytecode_deserialization = true,
    field = GenericField::new("root", GenericDataType::Union, false)
        .with_child(GenericField::new("U32", GenericDataType::U32, false))
        .with_child(GenericField::new("Bool", GenericDataType::Bool, false))
        .with_child(GenericField::new("Str", GenericDataType::LargeUtf8, false)),
    ty = U,
    values = [
        U::U32(32),
        U::Bool(true),
        U::Str(String::from("hello world")),
    ],
    nulls = [false, false, false],
    define = {
        #[derive(Serialize, Deserialize, Debug, PartialEq)]
        enum U {
            U32(u32),
            Bool(bool),
            Str(String),
        }
    },
);

test_example!(
    test_name = union_mixed,
    test_bytecode_deserialization = true,
    field =
        GenericField::new("root", GenericDataType::Union, false)
            .with_child(
                GenericField::new("V1", GenericDataType::Struct, false)
                    .with_child(GenericField::new("a", GenericDataType::U32, false))
                    .with_child(GenericField::new("b", GenericDataType::U64, false))
            )
            .with_child(GenericField::new("Bool", GenericDataType::Bool, false))
            .with_child(
                GenericField::new("S", GenericDataType::Struct, false)
                    .with_child(GenericField::new("s", GenericDataType::LargeUtf8, false))
            ),
    ty = U,
    values = [
        U::V1 { a: 32, b: 13 },
        U::Bool(true),
        U::S(S {
            s: String::from("hello world")
        })
    ],
    nulls = [false, false, false],
    define = {
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
    },
);

test_example!(
    test_name = union_nested,
    test_bytecode_deserialization = true,
    field = GenericField::new("root", GenericDataType::Union, false)
        .with_child(GenericField::new("U32", GenericDataType::U32, false))
        .with_child(
            GenericField::new("O", GenericDataType::Union, false)
                .with_child(GenericField::new("Bool", GenericDataType::Bool, false))
                .with_child(GenericField::new("Str", GenericDataType::LargeUtf8, false))
        ),
    ty = U,
    values = [
        U::U32(32),
        U::O(O::Bool(true)),
        U::O(O::Str(String::from("hello world"))),
        U::U32(16)
    ],
    nulls = [false, false, false, false],
    define = {
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
    },
);

test_example!(
    test_name = enums,
    test_bytecode_deserialization = true,
    field = GenericField::new("root", GenericDataType::Union, false)
        .with_child(GenericField::new("U8", GenericDataType::U8, false))
        .with_child(GenericField::new("U16", GenericDataType::U16, false))
        .with_child(GenericField::new("U32", GenericDataType::U32, false))
        .with_child(GenericField::new("U64", GenericDataType::U64, false)),
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
    test_bytecode_deserialization = true,
    field = GenericField::new("root", GenericDataType::Union, false)
        .with_child(
            GenericField::new("A", GenericDataType::Struct, false)
                .with_strategy(Strategy::TupleAsStruct)
                .with_child(GenericField::new("0", GenericDataType::U8, false))
                .with_child(GenericField::new("1", GenericDataType::U32, false))
        )
        .with_child(
            GenericField::new("B", GenericDataType::Struct, false)
                .with_strategy(Strategy::TupleAsStruct)
                .with_child(GenericField::new("0", GenericDataType::U16, false))
                .with_child(GenericField::new("1", GenericDataType::U64, false))
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
    test_bytecode_deserialization = true,
    field = GenericField::new("root", GenericDataType::Union, false)
        .with_child(
            GenericField::new("A", GenericDataType::Struct, false)
                .with_child(GenericField::new("a", GenericDataType::U8, false))
                .with_child(GenericField::new("b", GenericDataType::U32, false))
        )
        .with_child(
            GenericField::new("B", GenericDataType::Struct, false)
                .with_child(GenericField::new("c", GenericDataType::U16, false))
                .with_child(GenericField::new("d", GenericDataType::U64, false))
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
    test_bytecode_deserialization = true,
    tracing_options = TracingOptions::default().allow_null_fields(true),
    field = GenericField::new("root", GenericDataType::Union, false)
        .with_child(GenericField::new("A", GenericDataType::Null, true))
        .with_child(GenericField::new("B", GenericDataType::Null, true)),
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

test_error!(
    test_name = missing_union_variants,
    expected_error = "Serialization failed: an unknown variant",
    block = {
        use crate::schema::TracingOptions;
        use serde::{Deserialize, Serialize};

        #[derive(Serialize, Deserialize, Debug, PartialEq)]
        enum U {
            A,
            B,
            C,
        }

        let tracing_options = TracingOptions::default().allow_null_fields(true);
        let field = serialize_into_field(&[U::A, U::C], "root", tracing_options).unwrap();

        // NOTE: variant B was never encountered during tracing
        serialize_into_array(&field, &[U::A, U::B, U::C])?;

        Ok(())
    },
);

test_error!(
    test_name = missing_union_variant_compilation,
    expected_error = "Serialization failed: an unknown variant",
    block = {
        use crate::schema::TracingOptions;
        use crate::test_impls::utils::ScopedConfiguration;
        use serde::{Deserialize, Serialize};

        #[derive(Serialize, Deserialize, Debug, PartialEq)]
        enum U {
            A,
            B,
            C,
        }

        let _guard = ScopedConfiguration::configure(|c| {
            c.debug_print_program = true;
        });

        let tracing_options = TracingOptions::default().allow_null_fields(true);
        let field = serialize_into_field(&[U::A, U::C], "root", tracing_options).unwrap();

        // NOTE: variant B was never encountered during tracing
        serialize_into_array(&field, &[U::A, U::B, U::C])?;

        Ok(())
    },
);
