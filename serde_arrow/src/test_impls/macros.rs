macro_rules! btree_map {
    () => {
        ::std::collections::BTreeMap::new()
    };
    ($($key:expr => $value:expr),*) => {
        {
            let mut m = ::std::collections::BTreeMap::new();
            $(m.insert($key.into(), $value.into());)*
            m
        }
    };
}

pub(crate) use btree_map;

macro_rules! hash_map {
    () => {
        ::std::collections::HashMap::new()
    };
    ($($key:expr => $value:expr),*) => {
        {
            let mut m = ::std::collections::HashMap::new();
            $(m.insert($key.into(), $value.into());)*
            m
        }
    };
}

pub(crate) use hash_map;

macro_rules! test_example_impl {
    (
        test_name = $test_name:ident,
        test_compilation = $test_compilation:expr,
        $(tracing_options = $tracing_options:expr,)?
        field = $field:expr,
        $(overwrite_field = $overwrite_field:expr,)?
        ty = $ty:ty,
        values = $values:expr,
        $( nulls = $nulls:expr, )?
        $(define = { $($definitions:item)* } ,)?
    ) => {
        use std::collections::{BTreeMap, HashMap};
        use serde::Serialize;

        use super::*;

        use crate::{
            internal::schema::{
                GenericDataType,
                GenericField,
                Strategy,
                TracingOptions,
            },
            test_impls::macros::{btree_map, hash_map},
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
        fn compatible_fields() {
            $($($definitions)*)?

            let items: &[$ty] = &$values;
            let field = $field;
            $(let field = $overwrite_field;)?

            #[allow(unused)]
            let options = TracingOptions::default();
            $(let options = $tracing_options;)?

            println!("{options:?}");

            let traced = serialize_into_field(&items, "root", options).unwrap();
            let traced: GenericField = (&traced).try_into().unwrap();

            assert!(
                traced.is_compatible(&field),
                concat!(
                    "\n\n",
                    "[{test_name}] Incompatible fields.\n",
                    "Traced:  {traced:?}\n",
                    "Defined: {defined:?}\n",
                ),
                test_name = stringify!($test_name),
                traced = traced,
                defined = field,
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

        #[test]
        fn builder() {
            $($($definitions)*)?

            let items: &[$ty] = &$values;
            let field = $field;
            $(let field = $overwrite_field;)?
            let field: Field = (&field).try_into().unwrap();

            let array_reference = serialize_into_array(&field, &items).unwrap();

            let mut builder = ArrayBuilder::new(&field).unwrap();

            // build using extend
            builder.extend(items).unwrap();

            let array = builder.build_array().unwrap();
            assert_eq!(array.as_ref(), array_reference.as_ref());

            // re-use the builder
            for item in items {
                builder.push(item).unwrap();
            }

            let array = builder.build_array().unwrap();
            assert_eq!(array.as_ref(), array_reference.as_ref());
        }
    };
}

pub(crate) use test_example_impl;

macro_rules! test_compilation_impl {
    (
        test_name = $test_name:ident,
        test_compilation = true,
        $(tracing_options = $tracing_options:expr,)?
        field = $field:expr,
        $(overwrite_field = $overwrite_field:expr,)?
        ty = $ty:ty,
        values = $values:expr,
        $( nulls = $nulls:expr, )?
        $(define = { $($definitions:item)* } ,)?
    ) => {
        mod compilation {
            use std::collections::{BTreeMap, HashMap};
            use serde::Serialize;

            use super::*;

            use crate::{
                arrow::bytecode::build_array_data,
                base::serialize_into_sink,
                internal::{
                    bytecode::{compile_serialization, Interpreter, CompilationOptions},
                    schema::{
                        GenericDataType,
                        GenericField,
                        Strategy,
                        TracingOptions,
                    },
                },
                _impl::arrow::array::make_array,
                test_impls::macros::{btree_map, hash_map},
            };

            #[test]
            fn serialization() {
                $($($definitions)*)?

                let items: &[$ty] = &$values;
                let field = $field;
                $(let field = $overwrite_field;)?

                let program = compile_serialization(
                    &[field],
                    CompilationOptions::default().wrap_with_struct(false),
                ).unwrap();
                println!("{:?}", program.program);
                let mut interpreter = Interpreter::new(program);
                serialize_into_sink(&mut interpreter, items).unwrap();

                println!("{:?}", interpreter.array_mapping);
                println!("{:?}", interpreter.buffers);

                let arrays = interpreter.build_arrow_arrays().unwrap();

                assert_eq!(
                    arrays[0].len(),
                    items.len(),
                    "Unexpected length of array. Expected: {expected}. Actual: {actual}",
                    expected = items.len(),
                    actual = arrays[0].len(),
                );
            }
        }
    };

    (
        test_name = $test_name:ident,
        $($tt:tt)*
    ) => {};
}

pub(crate) use test_compilation_impl;

macro_rules! test_example {
    (
        test_name = $test_name:ident,
        $($tt:tt)*
    ) => {
        #[allow(unused)]
        mod $test_name {
            mod arrow {
                use crate::{
                    arrow::{serialize_into_field, serialize_into_array, ArrayBuilder},
                    _impl::arrow::datatypes::Field,
                };

                $crate::test_impls::macros::test_example_impl!(
                    test_name = $test_name,
                    $($tt)*
                );
            }
            mod arrow2 {
                use crate::{
                    arrow2::{serialize_into_field, serialize_into_array, ArrayBuilder},
                    _impl::arrow2::datatypes::Field,
                };

                $crate::test_impls::macros::test_example_impl!(
                    test_name = $test_name,
                    $($tt)*
                );
            }

            $crate::test_impls::macros::test_compilation_impl!(
                test_name = $test_name,
                $($tt)*
            );

        }
    };
}

pub(crate) use test_example;
