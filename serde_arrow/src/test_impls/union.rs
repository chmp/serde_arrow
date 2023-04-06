use super::macros::test_example;

test_example!(
    test_name = union_simple,
    field = GenericField::new("root", GenericDataType::Union, false)
        .with_child(GenericField::new("U32", GenericDataType::U32, false))
        .with_child(GenericField::new("Bool", GenericDataType::Bool, false))
        .with_child(GenericField::new("Str", GenericDataType::LargeUtf8, false)),
    ty = U,
    values = [U::U32(32), U::Bool(true), U::Str("hello world")],
    nulls = [false, false, false],
    define = {
        #[derive(Serialize)]
        enum U {
            U32(u32),
            Bool(bool),
            Str(&'static str),
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
        U::S(S { s: "hello world" })
    ],
    nulls = [false, false, false],
    define = {
        #[derive(Serialize)]
        enum U {
            V1 { a: u32, b: u64 },
            Bool(bool),
            S(S),
        }

        #[derive(Serialize)]
        struct S {
            s: &'static str,
        }
    },
);
