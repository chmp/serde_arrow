macro_rules! test_wrapper_impl {
    (
        $(#[ignore = $ignore:literal])?
        test_name = $test_name:ident,
        values = $values:expr,
    ) => {
        use super::*;

        use crate::{
            internal::schema::TracingOptions,
            test_impls::utils::ScopedConfiguration,
        };

        $(#[ignore = $ignore])?
        #[test]
        fn serialization() {
            let _guard = ScopedConfiguration::configure(|c| {
                c.debug_print_program = true;
            });

            let items = &$values;
            let field = serialize_into_field(&items, "root", TracingOptions::default()).unwrap();
            let array = serialize_into_array(&field, &items).unwrap();

            std::mem::drop(array);
        }

    };
}

macro_rules! test_wrapper {
    (
        $(#[ignore = $ignore:literal])?
        test_name = $test_name:ident,
        $($tt:tt)*
    ) => {
        #[allow(unused)]
        mod $test_name {
            mod arrow {
                use crate::{
                    arrow::{deserialize_from_array, serialize_into_field, serialize_into_array, ArrayBuilder},
                    _impl::arrow::datatypes::Field,
                };
                const IMPL: &'static str = "arrow";

                test_wrapper_impl!(
                    $(#[ignore = $ignore])?
                    test_name = $test_name,
                    $($tt)*
                );
            }
            mod arrow2 {
                use crate::{
                    arrow2::{deserialize_from_array, serialize_into_field, serialize_into_array, ArrayBuilder},
                    _impl::arrow2::datatypes::Field,
                };
                const IMPL: &'static str = "arrow2";

                test_wrapper_impl!(
                    $(#[ignore = $ignore])?
                    test_name = $test_name,
                    $($tt)*
                );
            }
        }
    };
}

test_wrapper!(test_name = outer_vec, values = vec![0_u32, 1_u32, 2_u32],);

test_wrapper!(test_name = outer_slice, values = &[0_u32, 1_u32, 2_u32],);

test_wrapper!(test_name = outer_array, values = [0_u32, 1_u32, 2_u32],);

test_wrapper!(test_name = outer_tuple, values = (0_u32, 1_u32, 2_u32),);
