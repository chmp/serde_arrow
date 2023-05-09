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
        $(test_deserialization = $test_deserialization:expr,)?
        $(tracing_options = $tracing_options:expr,)?
        field = $field:expr,
        $(overwrite_field = $overwrite_field:expr,)?
        ty = $ty:ty,
        values = $values:expr,
        $( nulls = $nulls:expr, )?
        $(define = { $($definitions:item)* } ,)?
    ) => {
        use std::collections::{BTreeMap, HashMap};
        use serde::{Serialize, Deserialize};

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

            let test_deserialization = true;
            $(let test_deserialization = $test_deserialization;)?

            // NOTE: dictionary sources are not yet supported
            if test_deserialization {
                let items_round_trip: Vec<$ty> = deserialize_from_array(&field, &array).unwrap();

                assert_eq!(items, items_round_trip);
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
        $(test_deserialization = $test_deserialization:expr,)?
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
            use serde::{Serialize, Deserialize};

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
                    sink::accept_events,
                },
                test_impls::{
                    macros::{btree_map, hash_map},
                    utils::deserialize_from_arrow_array,
                },
                _impl::arrow::datatypes::Field,
            };

            #[test]
            fn serialization() {
                $($($definitions)*)?

                let items: &[$ty] = &$values;
                let field = $field;
                $(let field = $overwrite_field;)?

                let program = compile_serialization(
                    &[field.clone()],
                    CompilationOptions::default().wrap_with_struct(false),
                ).unwrap();
                println!("structure: {:?}", program.structure);

                let mut events = Vec::new();
                serialize_into_sink(&mut events, items).unwrap();

                println!("events: {events:?}");

                let mut interpreter = Interpreter::new(program);
                accept_events(&mut interpreter, events).unwrap();

                println!("buffers: {:?}", interpreter.buffers);

                let arrays = interpreter.build_arrow_arrays().unwrap();

                assert_eq!(arrays.len(), 1);
                assert_eq!(
                    arrays[0].len(),
                    items.len(),
                    "Unexpected length of array. Expected: {expected}. Actual: {actual}",
                    expected = items.len(),
                    actual = arrays[0].len(),
                );

                let test_deserialization = true;
                $(let test_deserialization = $test_deserialization;)?

                // NOTE: dictionary sources are not yet supported
                if test_deserialization {
                    let arrow_field: Field = (&field).try_into().unwrap();
                    let items_round_trip: Vec<$ty> = deserialize_from_arrow_array(&arrow_field, &arrays[0]).unwrap();

                    assert_eq!(items, items_round_trip);
                }
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
                    test_impls::utils::deserialize_from_arrow_array as deserialize_from_array,
                };

                $crate::test_impls::macros::test_example_impl!(
                    test_name = $test_name,
                    $($tt)*
                );
            }
            mod arrow2 {
                use crate::{
                    arrow2::{deserialize_from_array, serialize_into_field, serialize_into_array, ArrayBuilder},
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
                bytecode::{compile_serialization, CompilationOptions, Interpreter},
                event::Event,
                schema::{GenericDataType, GenericField, Tracer, TracingOptions},
                sink::{accept_events, StripOuterSequenceSink},
            };

            #[test]
            fn tracing() {
                let events = &$events;
                let fields = &$fields;

                #[allow(unused)]
                let options = TracingOptions::default();
                $(let options = $tracing_options;)?

                let tracer = Tracer::new(String::from("$"), options);
                let mut tracer = StripOuterSequenceSink::new(tracer);
                accept_events(&mut tracer, events.iter().cloned()).unwrap();
                let root = tracer.into_inner().to_field("root").unwrap();

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
