use super::macros::test_generic;

test_generic!(
    #[ignore = "this is currently not implemented"]
    fn declared_but_missing_fields() {
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

        let arrays = serialize_into_arrays(&fields, &items).unwrap();

        assert_eq!(arrays.len(), 2);
        assert_eq!(arrays[0].len(), 2);
        assert_eq!(arrays[1].len(), 2);
    }
);
