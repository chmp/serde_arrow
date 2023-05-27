use super::macros::test_example;

test_example!(
    test_name = fieldless_unions,
    // NOTE: bytecode support requires more robust option handling
    test_compilation = [],
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
    test_name = union_simple,
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
