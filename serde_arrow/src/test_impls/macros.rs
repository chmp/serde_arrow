macro_rules! test_example_impl {
    (
        test_name = $test_name:ident,
        $(tracing_options = $tracing_options:expr,)?
        field = $field:expr,
        $(overwrite_field = $overwrite_field:expr,)?
        ty = $ty:ty,
        values = $values:expr,
        $( nulls = $nulls:expr, )?
        $(define = { $($definitions:item)* } ,)?
    ) => {
        use super::*;

        use crate::internal::schema::{
            GenericDataType,
            GenericField,
            Strategy,
            TracingOptions,
        };

        #[test]
        fn tracing() {
            $($($definitions)*)?

            let items: &[$ty] = &$values;
            let field = $field;

            #[allow(unused)]
            let options = TracingOptions::default();
            $(let options = $tracing_options;)?

            println!("{options:?}");

            let actual = serialize_into_field(&items, "root", options).unwrap();
            let expected: Field = (&field).try_into().unwrap();
            assert_eq!(
                actual,
                expected,
                concat!(
                    "\n\n",
                    "[{test_name}] Fields do not agree.\n",
                    "Actual:   {actual:?}\n",
                    "Expected: {expected:?}\n",
                ),
                test_name = stringify!($test_name),
                actual = actual,
                expected = expected,
            );
        }

        #[test]
        fn serialization() {
            $($($definitions)*)?

            let items: &[$ty] = &$values;
            let field = $field;
            $(let field = $overwrite_field;)?
            let field: Field = (&field).try_into().unwrap();

            let array = serialize_into_array(&field, &items).unwrap();
            assert_eq!(array.data_type(), field.data_type());
            assert_eq!(array.len(), items.len());

            let test_null = false;
            let expected_nulls: Vec<bool> = vec![];
            $(
                let test_null = true;
                let expected_nulls: Vec<bool> = $nulls.to_vec();
            )?
            if test_null {
                let actual_nulls: Vec<bool> = (0..array.len()).map(|idx| array.is_null(idx)).collect();
                assert_eq!(
                    actual_nulls,
                    expected_nulls,
                    concat!(
                        "\n\n",
                        "[{test_name}] Null bitmaps do no agree.\n",
                        "Actual:   {actual:?}\n",
                        "Expected: {expected:?}\n",
                    ),
                    test_name = stringify!($test_name),
                    actual = actual_nulls,
                    expected = expected_nulls,
                );
            }
        }
    };
}

pub(crate) use test_example_impl;

macro_rules! test_example {
    (
        test_name = $test_name:ident,
        $($tt:tt)*
    ) => {
        #[allow(unused)]
        mod $test_name {
            mod arrow {
                use serde::Serialize;

                use crate::{
                    arrow::{serialize_into_field, serialize_into_array},
                    impls::arrow::schema::Field,
                };

                $crate::test_impls::macros::test_example_impl!(
                    test_name = $test_name,
                    $($tt)*
                );
            }
            mod arrow2 {
                use serde::Serialize;

                use crate::{
                    arrow2::{serialize_into_field, serialize_into_array},
                    impls::arrow2::datatypes::Field,
                };

                $crate::test_impls::macros::test_example_impl!(
                    test_name = $test_name,
                    $($tt)*
                );
            }
        }
    };
}

pub(crate) use test_example;
