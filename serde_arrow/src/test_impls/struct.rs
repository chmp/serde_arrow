use super::macros::*;

test_example!(
    test_name = struct_,
    test_bytecode_deserialization = false,
    field = GenericField::new("root", GenericDataType::Struct, false)
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
    test_bytecode_deserialization = false,
    field = GenericField::new("root", GenericDataType::Struct, false)
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
    test_bytecode_deserialization = false,
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
        #[derive(Serialize, Deserialize, Debug, PartialEq)]
        struct S {
            a: Option<u32>,
            b: bool,
        }
    },
);

test_example!(
    test_name = nullable_struct,
    test_bytecode_deserialization = false,
    field = GenericField::new("root", GenericDataType::Struct, true)
        .with_child(GenericField::new("a", GenericDataType::U32, false))
        .with_child(GenericField::new("b", GenericDataType::Bool, false)),
    ty = Option<S>,
    values = [Some(S { a: 1, b: true }), None],
    nulls = [false, true],
    define = {
        #[derive(Serialize, Deserialize, Debug, PartialEq)]
        struct S {
            a: u32,
            b: bool,
        }
    },
);

test_example!(
    test_name = nullable_struct_nullable_fields,
    test_bytecode_deserialization = false,
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
//     field = GenericField::new("root", GenericDataType::Struct, false),
//     ty = S,
//     values = [S {}, S {}, S {}],
//     nulls = [false, false, false],
//     define = {
//         #[derive(Serialize)]
//         struct S {}
//     },
// );

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
