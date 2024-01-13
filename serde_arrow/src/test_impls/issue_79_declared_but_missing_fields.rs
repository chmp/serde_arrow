use serde::Serialize;
use serde_json::json;

use crate::test_impls::utils::Test;

use super::macros::test_generic;

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

test_generic!(
    fn declared_but_missing_fields_non_nullable() {
        use serde::Serialize;

        #[derive(Serialize)]
        struct S {
            a: u8,
        }

        let items = [S { a: 0 }, S { a: 1 }];

        let fields = vec![
            Field::try_from(&GenericField::new("a", GenericDataType::U8, false)).unwrap(),
            Field::try_from(&GenericField::new("b", GenericDataType::U8, false)).unwrap(),
        ];

        let Err(err) = to_arrow(&fields, &items) else {
            panic!("Expected error");
        };
        assert!(
            err.to_string()
                .contains("missing non-nullable field b in struct"),
            "unexpected error: {err}"
        );
    }
);
