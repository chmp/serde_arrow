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
        $(#[ignore = $ignore:literal])?
        test_name = $test_name:ident,
        $(test_bytecode_deserialization = $test_bytecode_deserialization:expr,)?
        $(test_deserialization = $test_deserialization:expr,)?
        $(tracing_options = $tracing_options:expr,)?
        field = $field:expr,
        $(overwrite_field = $overwrite_field:expr,)?
        ty = $ty:ty,
        values = $values:expr,
        $(expected_values = $expected_values:expr,)?
        $( nulls = $nulls:expr, )?
        $(define = { $($definitions:item)* } ,)?
    ) => {
        use std::collections::{BTreeMap, HashMap};
        use serde::{Serialize, Deserialize};

        use super::*;

        use crate::{
            schema::{SerdeArrowSchema, TracingOptions, Strategy},
            utils::{Items, Item},
        };
        use crate::internal::schema::{GenericDataType, GenericField, GenericTimeUnit};
        use crate::test_impls::macros::{btree_map, hash_map};

        $(#[ignore = $ignore])?
        #[test]
        fn tracing() {
            $($($definitions)*)?

            let items: &[$ty] = &$values;
            let field = $field;

            #[allow(unused)]
            let options = TracingOptions::default();
            $(let options = $tracing_options;)?

            println!("{options:?}");

            let actual: Vec<Field> = SerdeArrowSchema::from_samples(&Items(items), options).unwrap().try_into().unwrap();
            let expected: Vec<Field> = vec![(&field).try_into().unwrap()];
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

            let traced: GenericField = (&actual[0]).try_into().unwrap();
            println!("traced: {:?}\n", traced);
            println!("defined: {:?}\n", field);

            traced.validate_compatibility(&field).unwrap();
        }

        $(#[ignore = $ignore])?
        #[test]
        fn serialization() {
            $($($definitions)*)?

            let items: &[$ty] = &$values;
            let field = $field;
            $(let field = $overwrite_field;)?
            let field: Field = (&field).try_into().unwrap();

            let arrays = to_arrow(std::slice::from_ref(&field), &Items(items)).unwrap();
            let array = arrays.into_iter().next().unwrap();
            assert_eq!(array.data_type(), field.data_type(), "Unexpected data type");
            assert_eq!(array.len(), items.len(), "Unexpected number of items");

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

            let test_deserialization: &[&str] = &["arrow", "arrow2"];
            $(let test_deserialization: &[&str] = &$test_deserialization;)?

            if test_deserialization.contains(&IMPL) {
                let expected_items = items;
                $(let expected_items: &[$ty] = &$expected_values;)?

                let Items(items_round_trip): Items<Vec<$ty>> = from_arrow(
                    std::slice::from_ref(&field),
                    std::slice::from_ref(&array),
                ).unwrap();
                assert_eq!(expected_items, items_round_trip);
            }
        }

        $(#[ignore = $ignore])?
        #[test]
        fn builder() {
            $($($definitions)*)?

            let items: &[$ty] = &$values;
            let field = $field;
            $(let field = $overwrite_field;)?
            let field: Field = (&field).try_into().unwrap();

            let arrays_reference = to_arrow(std::slice::from_ref(&field), &Items(items)).unwrap();
            let array_reference = arrays_reference.into_iter().next().unwrap();

            let mut builder = ArrowBuilder::new(std::slice::from_ref(&field)).unwrap();

            // build using extend
            builder.extend(&Items(items)).unwrap();

            let arrays = builder.build_arrays().unwrap();
            let array = arrays.into_iter().next().unwrap();
            assert_eq!(array.as_ref(), array_reference.as_ref());

            // re-use the builder
            for item in items {
                builder.push(&Item(item)).unwrap();
            }

            let arrays = builder.build_arrays().unwrap();
            let array = arrays.into_iter().next().unwrap();
            assert_eq!(array.as_ref(), array_reference.as_ref());

            let test_deserialization: &[&str] = &["arrow", "arrow2"];
            $(let test_deserialization: &[&str] = &$test_deserialization;)?

            if test_deserialization.contains(&IMPL) {
                let expected_items = items;
                $(let expected_items: &[$ty] = &$expected_values;)?

                let Items(items_round_trip): Items<Vec<$ty>> = from_arrow(
                    std::slice::from_ref(&field),
                    std::slice::from_ref(&array),
                ).unwrap();
                assert_eq!(expected_items, items_round_trip);
            }
        }
    };
}

pub(crate) use test_example_impl;

/// Test conversion of a single array
///
/// This macro supports the following syntax:
///
/// ```rust,ignore
/// test_example!(
///     test_name = $test_name:ident,
///     $(test_deserialization = $test_deserialization:expr,)?
///     $(tracing_options = $tracing_options:expr,)?
///     field = $field:expr,
///     $(overwrite_field = $overwrite_field:expr,)?
///     ty = $ty:ty,
///     values = $values:expr,
///     $(expected_values = $expected_values:expr,)?
///     $( nulls = $nulls:expr, )?
///     $(define = { $($definitions:item)* } ,)?
/// );
/// ```
macro_rules! test_example {
    (
        $(#[ignore = $ignore:literal])?
        test_name = $test_name:ident,
        $($tt:tt)*
    ) => {
        #[allow(unused)]
        mod $test_name {
            mod arrow {
                use crate::{
                    ArrowBuilder, to_arrow, from_arrow,
                    _impl::arrow::datatypes::Field,
                };
                const IMPL: &'static str = "arrow";

                $crate::test_impls::macros::test_example_impl!(
                    $(#[ignore = $ignore])?
                    test_name = $test_name,
                    $($tt)*
                );
            }
            mod arrow2 {
                use crate::{
                    Arrow2Builder as ArrowBuilder,
                    to_arrow2 as to_arrow,
                    from_arrow2 as from_arrow,
                    _impl::arrow2::datatypes::Field,
                };
                const IMPL: &'static str = "arrow2";

                $crate::test_impls::macros::test_example_impl!(
                    $(#[ignore = $ignore])?
                    test_name = $test_name,
                    $($tt)*
                );
            }
        }
    };
}

pub(crate) use test_example;

macro_rules! test_events {
    (
        test_name = $test_name:ident,
        $(tracing_options = $tracing_options:expr,)?
        fields = $fields:expr,
        $(overwrite_fields = $overwrite_fields:expr,)?
        events = $events:expr,
    ) => {
        mod $test_name {
            use crate::internal::{
                serialization::{compile_serialization, CompilationOptions, Interpreter},
                event::Event,
                schema::{GenericDataType, GenericField},
                tracing::{Tracer, TracingOptions},
                sink::{accept_events, StripOuterSequenceSink},
            };

            #[test]
            fn tracing() {
                let events = &$events;
                let fields = &$fields;

                #[allow(unused)]
                let options = TracingOptions::default();
                $(let options = $tracing_options;)?

                let mut tracer = Tracer::new(String::from("$"), options);
                let mut sink = StripOuterSequenceSink::new(&mut tracer);
                accept_events(&mut sink, events.iter().cloned()).unwrap();
                let root = tracer.to_field("item").unwrap();

                assert_eq!(root.children, fields);
            }

            #[test]
            fn serialize() {
                let events = &$events;

                #[allow(unused)]
                let fields = &$fields;
                $(let fields = &$overwrite_fields;)?

                let program = compile_serialization(fields, CompilationOptions::default()).unwrap();
                println!("sturcture: {:?}", program.structure);

                let mut interpreter = Interpreter::new(program);
                accept_events(&mut interpreter, events.iter().cloned()).unwrap();

                println!("buffers: {:?}", interpreter.buffers);

                interpreter.build_arrow_arrays().unwrap();
            }
        }
    };
}

pub(crate) use test_events;

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
            mod arrow2 {
                use serde::{Serialize, Deserialize};
                use crate::{
                    Arrow2Builder, to_arrow2, from_arrow2,
                    internal::schema::{GenericField, GenericDataType},
                    Result,
                };
                use crate::_impl::arrow2::datatypes::Field;

                #[test]
                fn serialize() {
                    $($setup)*

                    let fields = $fields;
                    let inputs = $inputs;

                    let expected = inputs;
                    $(let expected = $expected;)?

                    let fields = fields.iter().map(|f| Field::try_from(f)).collect::<Result<Vec<_>>>().unwrap();

                    let arrays = to_arrow2(&fields, inputs).unwrap();
                    let reconstructed: Vec<S> = from_arrow2(&fields, &arrays).unwrap();

                    assert_eq!(reconstructed, expected);
                }

                #[test]
                fn builder_push() {
                    $($setup)*

                    let fields = $fields;
                    let inputs = $inputs;

                    let expected = inputs;
                    $(let expected = $expected;)?

                    let fields = fields.iter().map(|f| Field::try_from(f)).collect::<Result<Vec<_>>>().unwrap();

                    let mut builder = Arrow2Builder::new(&fields).unwrap();

                    for item in inputs.iter() {
                        builder.push(item).unwrap();
                    }

                    let arrays = builder.build_arrays().unwrap();
                    let reconstructed: Vec<S> = from_arrow2(&fields, &arrays).unwrap();

                    assert_eq!(reconstructed, expected);
                }

                #[test]
                fn builder_extend() {
                    $($setup)*

                    let fields = $fields;
                    let inputs = $inputs;

                    let expected = inputs;
                    $(let expected = $expected;)?

                    let fields = fields.iter().map(|f| Field::try_from(f)).collect::<Result<Vec<_>>>().unwrap();

                    let mut builder = Arrow2Builder::new(&fields).unwrap();
                    builder.extend(inputs).unwrap();

                    let arrays = builder.build_arrays().unwrap();
                    let reconstructed: Vec<S> = from_arrow2(&fields, &arrays).unwrap();

                    assert_eq!(reconstructed, expected);
                }
            }
        }
    };
}

pub(crate) use test_roundtrip_arrays;

macro_rules! test_generic {
    (
        $(#[ignore = $ignore:literal])?
        fn $name:ident() {
            $($stmt:stmt)*
        }
    ) => {
        #[allow(unused)]
        mod $name {
            use crate::{
                schema::{SerdeArrowSchema, TracingOptions},
                utils::{Items, Item}
            };
            use crate::internal::schema::{GenericField, GenericDataType};

            mod arrow {
                use super::*;
                use crate::{to_arrow, from_arrow};
                use crate::_impl::arrow::datatypes::Field;

                $(#[ignore = $ignore])?
                #[test]
                fn test() {
                    $($stmt)*
                }
            }
            mod arrow2 {
                use super::*;
                use crate::{to_arrow2 as to_arrow, from_arrow2 as from_arrow};
                use crate::_impl::arrow2::datatypes::Field;

                $(#[ignore = $ignore])?
                #[test]
                fn test() {
                    $($stmt)*
                }
            }
        }
    };
}

pub(crate) use test_generic;

pub fn expect_error<T, E: std::fmt::Display>(actual: &Result<T, E>, expected: &str) {
    let Err(actual) = actual else {
        panic!("expected an error, but no error was raised");
    };

    let actual = actual.to_string();
    if !actual.contains(expected) {
        panic!("Error did not contain {expected:?}. Full error: {actual}");
    }
}
