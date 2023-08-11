use super::macros::test_generic;

macro_rules! test_missing_field {
    ($name:ident : $items:expr) => {
        test_generic!(
            fn $name() {
                use serde::Serialize;

                let items = $items;
                let fields = [
                    Field::try_from(&GenericField::new("a", GenericDataType::U8, false)).unwrap(),
                ];

                let res = serialize_into_arrays(&fields, &items).unwrap();
                assert_eq!(res.len(), 1);
                assert_eq!(res[0].len(), items.len());
            }
        );
    };
}

test_missing_field!(missing_i8: {
    #[derive(Serialize, Debug, PartialEq)]
    struct S {
        a: i16,
        b: i8,
    }
    [S{a: 1, b: 2}, S{a: 3, b: 4}]
});

test_missing_field!(missing_i16: {
    #[derive(Serialize, Debug, PartialEq)]
    struct S {
        a: i16,
        b: i16,
    }
    [S{a: 1, b: 2}, S{a: 3, b: 4}]
});

test_missing_field!(missing_i32: {
    #[derive(Serialize, Debug, PartialEq)]
    struct S {
        a: i16,
        b: i32,
    }
    [S{a: 1, b: 2}, S{a: 3, b: 4}]
});

test_missing_field!(missing_i64: {
    #[derive(Serialize, Debug, PartialEq)]
    struct S {
        a: i16,
        b: i64,
    }
    [S{a: 1, b: 2}, S{a: 3, b: 4}]
});

test_missing_field!(missing_u8: {
    #[derive(Serialize, Debug, PartialEq)]
    struct S {
        a: i16,
        b: u8,
    }
    [S{a: 1, b: 2}, S{a: 3, b: 4}]
});

test_missing_field!(missing_u16: {
    #[derive(Serialize, Debug, PartialEq)]
    struct S {
        a: i16,
        b: u16,
    }
    [S{a: 1, b: 2}, S{a: 3, b: 4}]
});

test_missing_field!(missing_u32: {
    #[derive(Serialize, Debug, PartialEq)]
    struct S {
        a: i16,
        b: u32,
    }
    [S{a: 1, b: 2}, S{a: 3, b: 4}]
});

test_missing_field!(missing_u64: {
    #[derive(Serialize, Debug, PartialEq)]
    struct S {
        a: i16,
        b: u64,
    }
    [S{a: 1, b: 2}, S{a: 3, b: 4}]
});

test_missing_field!(missing_f32: {
    #[derive(Serialize, Debug, PartialEq)]
    struct S {
        a: i16,
        b: f32,
    }
    [S{a: 1, b: 2.0}, S{a: 3, b: 4.0}]
});

test_missing_field!(missing_f64: {
    #[derive(Serialize, Debug, PartialEq)]
    struct S {
        a: i16,
        b: f32,
    }
    [S{a: 1, b: 2.0}, S{a: 3, b: 4.0}]
});

test_missing_field!(missing_bool: {
    #[derive(Serialize, Debug, PartialEq)]
    struct S {
        a: i16,
        b: bool,
    }
    [S{a: 1, b: true}, S{a: 3, b: false}]
});

test_missing_field!(missing_str: {
    #[derive(Serialize, Debug, PartialEq)]
    struct S {
        a: i16,
        b: String,
    }
    [S{a: 1, b: String::from("hello")}, S{a: 3, b: String::from("world")}]
});

test_missing_field!(optional_u32: {
    #[derive(Serialize, Debug, PartialEq)]
    struct S {
        a: i16,
        b: Option<u32>,
    }
    [S{a: 1, b: Some(2)}, S{a: 3, b: None}]
});

test_missing_field!(vec_i64: {
    #[derive(Serialize, Debug, PartialEq)]
    struct S {
        a: i16,
        b: Vec<i64>,
    }
    [S{a: 1, b: vec![]}, S{a: 3, b: vec![1]}, S{a: 3, b: vec![1, 2]}]
});

test_missing_field!(nested_struct: {
    #[derive(Serialize, Debug, PartialEq)]
    struct S {
        a: i16,
        b: T,
    }

    #[derive(Serialize, Debug, PartialEq)]
    struct T {
        b: Vec<i64>,
        c: bool,
    }

    [S{a: 1, b: T { b: vec![], c: true}}, S{a: 3, b: T { b: vec![1, 2], c: false }}]
});

test_missing_field!(tuple: {
    #[derive(Serialize, Debug, PartialEq)]
    struct S {
        a: i16,
        b: (i32, T),
    }

    #[derive(Serialize, Debug, PartialEq)]
    struct T {
        b: Vec<i64>,
        c: bool,
    }

    [S{a: 1, b: (0, T { b: vec![], c: true})}, S{a: 3, b: (1, T { b: vec![1, 2], c: false })}]
});
