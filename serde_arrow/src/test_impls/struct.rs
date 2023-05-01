use super::macros::*;

test_example!(
    test_name = struct_,
    test_compilation = true,
    field = GenericField::new("root", GenericDataType::Struct, false)
        .with_child(GenericField::new("a", GenericDataType::U32, false))
        .with_child(GenericField::new("b", GenericDataType::Bool, false)),
    ty = S,
    values = [S { a: 1, b: true }, S { a: 2, b: false }],
    nulls = [false, false],
    define = {
        #[derive(Serialize)]
        struct S {
            a: u32,
            b: bool,
        }
    },
);

test_example!(
    test_name = struct_nullable_field,
    test_compilation = true,
    field = GenericField::new("root", GenericDataType::Struct, false)
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
        #[derive(Serialize)]
        struct S {
            a: Option<u32>,
            b: bool,
        }
    },
);

test_example!(
    test_name = nullable_struct,
    test_compilation = true,
    field = GenericField::new("root", GenericDataType::Struct, true)
        .with_child(GenericField::new("a", GenericDataType::U32, false))
        .with_child(GenericField::new("b", GenericDataType::Bool, false)),
    ty = Option<S>,
    values = [Some(S { a: 1, b: true }), None],
    nulls = [false, true],
    define = {
        #[derive(Serialize)]
        struct S {
            a: u32,
            b: bool,
        }
    },
);

test_example!(
    test_name = nullable_struct_nullable_fields,
    test_compilation = true,
    field = GenericField::new("root", GenericDataType::Struct, true)
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
        #[derive(Serialize)]
        struct S {
            a: Option<u32>,
            b: Option<bool>,
        }
    },
);

// arrow2 panics with: OutOfSpec("A StructArray must contain at least one field")
// test_example!(
//     test_name = empt_struct,
//     field = GenericField::new("root", GenericDataType::Struct, false),
//     ty = S,
//     values = [S {}, S {}, S {}],
//     nulls = [false, false, false],
//     define = {
//         #[derive(Serialize)]
//         struct S {}
//     },
// );
