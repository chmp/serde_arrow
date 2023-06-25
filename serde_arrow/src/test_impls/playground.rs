macro_rules! test_roundtrip_arrays {
    (
        $name:ident {
            $($setup:tt)*
        }
        assert_round_trip(
            $fields:expr,
            $inputs:expr
            $(, expected: $expected:expr)?
        );
    ) => {
        mod $name {
            use serde::{Deserialize, Serialize};

            use crate::{
                arrow, arrow2,
                internal::{
                    deserialize_from_arrays,
                    schema::{GenericDataType, GenericField},
                },
                Result,
            };

            #[test]
            fn arrow2() {
                use crate::_impl::arrow2::datatypes::Field;
                $($setup)*

                let fields = $fields;
                let inputs = $inputs;

                let expected = inputs;
                $(let expected = $expected;)?


                let fields = fields.iter().map(|f| Field::try_from(f)).collect::<Result<Vec<_>>>().unwrap();
                let arrays = arrow2::serialize_into_arrays(&fields, inputs).unwrap();

                let reconstructed: Vec<S> = deserialize_from_arrays(&fields, &arrays).unwrap();
                assert_eq!(reconstructed, expected);
            }

            #[test]
            fn arrow() {
                use crate::_impl::arrow::datatypes::Field;
                $($setup)*

                let fields = $fields;
                let inputs = $inputs;

                let expected = inputs;
                $(let expected = $expected;)?

                let fields = fields.iter().map(|f| Field::try_from(f)).collect::<Result<Vec<_>>>().unwrap();
                let arrays = arrow::serialize_into_arrays(&fields, inputs).unwrap();

                let reconstructed: Vec<S> = deserialize_from_arrays(&fields, &arrays).unwrap();
                assert_eq!(reconstructed, expected);
            }
        }
    };
}

test_roundtrip_arrays!(
    example {
        #[derive(Debug, PartialEq, Deserialize, Serialize)]
        struct S {
            a: i32,
            b: f32,
        }

        let items = &[S { a: 0, b: 2.0 }, S { a: 1, b: 3.0 }, S { a: 2, b: 4.0 }];

        let fields = vec![
            GenericField::new("a", GenericDataType::I32, false),
            GenericField::new("b", GenericDataType::F16, false),
        ];
    }
    assert_round_trip(fields, items);
);

test_roundtrip_arrays!(
    primitives {
        #[derive(Debug, Default, PartialEq, Deserialize, Serialize)]
        struct S {
            a: u8,
            b: u16,
            c: u32,
            d: u64,
            e: u8,
            f: u16,
            g: u32,
            h: u64,
            i: f32,
            j: f32,
            k: f64,
        }

        let items = &[
            S::default(),
            S::default(),
            S::default(),
        ];

        let fields = vec![
            GenericField::new("a", GenericDataType::U8, false),
            GenericField::new("b", GenericDataType::U16, false),
            GenericField::new("c", GenericDataType::U32, false),
            GenericField::new("d", GenericDataType::U64, false),
            GenericField::new("e", GenericDataType::I8, false),
            GenericField::new("f", GenericDataType::I16, false),
            GenericField::new("g", GenericDataType::I32, false),
            GenericField::new("h", GenericDataType::I64, false),
            GenericField::new("i", GenericDataType::F16, false),
            GenericField::new("j", GenericDataType::F32, false),
            GenericField::new("k", GenericDataType::F64, false),
        ];
    }
    assert_round_trip(fields, items);
);

test_roundtrip_arrays!(
    example_field_order_different_from_struct {
        #[derive(Debug, PartialEq, Deserialize, Serialize)]
        struct S {
            a: i32,
            b: f32,
        }

        let items = &[S { a: 0, b: 2.0 }, S { a: 1, b: 3.0 }, S { a: 2, b: 4.0 }];

        let fields = vec![
            GenericField::new("b", GenericDataType::F16, false),
            GenericField::new("a", GenericDataType::I32, false),
        ];
    }
    assert_round_trip(fields, items);
);

test_roundtrip_arrays!(
    example_optional_fields {
        #[derive(Debug, PartialEq, Deserialize, Serialize)]
        struct S {
            a: Option<i32>,
        }

        let items = &[S { a: Some(0) }, S { a: None }, S { a: Some(2) }];

        let fields = vec![
            GenericField::new("a", GenericDataType::I32, true),
        ];
    }
    assert_round_trip(fields, items);
);
