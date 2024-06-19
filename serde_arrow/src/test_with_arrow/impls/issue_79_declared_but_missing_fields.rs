use serde::Serialize;
use serde_json::json;

use super::utils::Test;
use crate::internal::testing::ResultAsserts;

#[test]
fn declared_but_missing_fields() {
    #[derive(Serialize)]
    struct S {
        a: u8,
    }

    let items = [S { a: 0 }, S { a: 1 }];

    Test::new()
        .with_schema(json!([
            {"name": "a", "data_type": "U8"},
            {"name": "b", "data_type": "U8", "nullable": true},
        ]))
        .serialize(&items)
        .also(|it| {
            let arrays = it.arrays.arrow.as_ref().unwrap();

            assert_eq!(arrays.len(), 2);
            assert_eq!(arrays[0].len(), 2);
            assert_eq!(arrays[1].len(), 2);
        });
}

#[test]
fn declared_but_missing_fields_non_nullable() {
    use serde::Serialize;

    #[derive(Serialize)]
    struct S {
        a: u8,
    }

    let items = [S { a: 0 }, S { a: 1 }];

    let mut test = Test::new().with_schema(json!([
        {"name": "a", "data_type": "U8"},
        {"name": "b", "data_type": "U8"},
    ]));

    test.try_serialize_arrow(&items)
        .assert_error("missing non-nullable field \"b\" in struct");
}
