use super::macros::{test_error, test_example};

test_example!(
    test_name = fieldless_unions,
    test_bytecode_deserialization = false,
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
    test_bytecode_deserialization = false,
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
    test_bytecode_deserialization = false,
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
    test_bytecode_deserialization = false,
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
    test_bytecode_deserialization = false,
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
