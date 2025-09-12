use super::utils::Test;

use serde::Serialize;
use serde_json::json;

#[test]
fn missing_i8() {
    #[derive(Serialize, Debug, PartialEq)]
    struct S {
        a: i16,
        b: i8,
    }

    Test::new()
        .with_schema(json!([{"name": "a", "data_type": "I16"}]))
        .serialize(&[S { a: 1, b: 2 }, S { a: 3, b: 4 }]);
}

#[test]
fn missing_i16() {
    #[derive(Serialize, Debug, PartialEq)]
    struct S {
        a: i16,
        b: i16,
    }

    Test::new()
        .with_schema(json!([{"name": "a", "data_type": "I16"}]))
        .serialize(&[S { a: 1, b: 2 }, S { a: 3, b: 4 }]);
}

#[test]
fn missing_i32() {
    #[derive(Serialize, Debug, PartialEq)]
    struct S {
        a: i16,
        b: i32,
    }

    Test::new()
        .with_schema(json!([{"name": "a", "data_type": "I16"}]))
        .serialize(&[S { a: 1, b: 2 }, S { a: 3, b: 4 }]);
}

#[test]
fn missing_i64() {
    #[derive(Serialize, Debug, PartialEq)]
    struct S {
        a: i16,
        b: i64,
    }

    Test::new()
        .with_schema(json!([{"name": "a", "data_type": "I16"}]))
        .serialize(&[S { a: 1, b: 2 }, S { a: 3, b: 4 }]);
}

#[test]
fn missing_u8() {
    #[derive(Serialize, Debug, PartialEq)]
    struct S {
        a: i16,
        b: u8,
    }

    Test::new()
        .with_schema(json!([{"name": "a", "data_type": "I16"}]))
        .serialize(&[S { a: 1, b: 2 }, S { a: 3, b: 4 }]);
}

#[test]
fn missing_u16() {
    #[derive(Serialize, Debug, PartialEq)]
    struct S {
        a: i16,
        b: u16,
    }

    Test::new()
        .with_schema(json!([{"name": "a", "data_type": "I16"}]))
        .serialize(&[S { a: 1, b: 2 }, S { a: 3, b: 4 }]);
}

#[test]
fn missing_u32() {
    #[derive(Serialize, Debug, PartialEq)]
    struct S {
        a: i16,
        b: u32,
    }

    Test::new()
        .with_schema(json!([{"name": "a", "data_type": "I16"}]))
        .serialize(&[S { a: 1, b: 2 }, S { a: 3, b: 4 }]);
}

#[test]
fn missing_u64() {
    #[derive(Serialize, Debug, PartialEq)]
    struct S {
        a: i16,
        b: u64,
    }

    Test::new()
        .with_schema(json!([{"name": "a", "data_type": "I16"}]))
        .serialize(&[S { a: 1, b: 2 }, S { a: 3, b: 4 }]);
}

#[test]
fn missing_f32() {
    #[derive(Serialize, Debug, PartialEq)]
    struct S {
        a: i16,
        b: f32,
    }

    Test::new()
        .with_schema(json!([{"name": "a", "data_type": "I16"}]))
        .serialize(&[S { a: 1, b: 2.0 }, S { a: 3, b: 4.0 }]);
}

#[test]
fn missing_f64() {
    #[derive(Serialize, Debug, PartialEq)]
    struct S {
        a: i16,
        b: f64,
    }

    Test::new()
        .with_schema(json!([{"name": "a", "data_type": "I16"}]))
        .serialize(&[S { a: 1, b: 2.0 }, S { a: 3, b: 4.0 }]);
}

#[test]
fn missing_bool() {
    #[derive(Serialize, Debug, PartialEq)]
    struct S {
        a: i16,
        b: bool,
    }

    Test::new()
        .with_schema(json!([{"name": "a", "data_type": "I16"}]))
        .serialize(&[S { a: 1, b: true }, S { a: 3, b: false }]);
}

#[test]
fn missing_string() {
    #[derive(Serialize, Debug, PartialEq)]
    struct S {
        a: i16,
        b: String,
    }

    Test::new()
        .with_schema(json!([{"name": "a", "data_type": "I16"}]))
        .serialize(&[
            S {
                a: 1,
                b: String::from("hello"),
            },
            S {
                a: 3,
                b: String::from("world"),
            },
        ]);
}

#[test]
fn missing_optional_u32() {
    #[derive(Serialize, Debug, PartialEq)]
    struct S {
        a: i16,
        b: Option<u32>,
    }

    Test::new()
        .with_schema(json!([{"name": "a", "data_type": "I16"}]))
        .serialize(&[S { a: 1, b: None }, S { a: 3, b: Some(4) }]);
}

#[test]
fn missing_optional_veci64() {
    #[derive(Serialize, Debug, PartialEq)]
    struct S {
        a: i16,
        b: Vec<i64>,
    }

    Test::new()
        .with_schema(json!([{"name": "a", "data_type": "I16"}]))
        .serialize(&[
            S { a: 1, b: vec![] },
            S {
                a: 3,
                b: vec![2, 4],
            },
        ]);
}

#[test]
fn missing_nested_struct() {
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

    Test::new()
        .with_schema(json!([{"name": "a", "data_type": "I16"}]))
        .serialize(&[
            S {
                a: 1,
                b: T { b: vec![], c: true },
            },
            S {
                a: 3,
                b: T {
                    b: vec![1, 2],
                    c: false,
                },
            },
        ]);
}

#[test]
fn missing_tuple() {
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

    Test::new()
        .with_schema(json!([{"name": "a", "data_type": "I16"}]))
        .serialize(&[
            S {
                a: 1,
                b: (0, T { b: vec![], c: true }),
            },
            S {
                a: 3,
                b: (
                    1,
                    T {
                        b: vec![1, 2],
                        c: false,
                    },
                ),
            },
        ]);
}

#[test]
fn missing_nested_field() {
    #[derive(Serialize, Debug, PartialEq)]
    struct S {
        a: i16,
        b: T,
    }

    #[derive(Serialize, Debug, PartialEq)]
    struct T {
        b: i32,
        c: bool,
    }

    Test::new()
        .with_schema(json!([
            {"name": "a", "data_type": "I16"},
            {
                "name": "b",
                "data_type": "Struct",
                "children": [
                    {"name": "b", "data_type": "I32"},
                ],
            },
        ]))
        .serialize(&[
            S {
                a: 1,
                b: T { b: 0, c: true },
            },
            S {
                a: 3,
                b: T { b: 1, c: false },
            },
        ]);
}
