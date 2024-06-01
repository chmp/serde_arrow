macro_rules! assert_error {
    ($res:expr, $expected:expr $(,)?) => {
        let Err(err) = $res else {
            panic!("Expected error");
        };
        assert!(
            err.to_string().contains($expected),
            "Unexpected error: {err}",
        );
    };
}

pub(crate) use assert_error;

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
                schema::{SchemaLike, TracingOptions},
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
