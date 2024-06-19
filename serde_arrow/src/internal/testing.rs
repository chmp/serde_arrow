//! Support for tests
use crate::internal::error::Result;

pub trait ResultAsserts {
    fn assert_error(&self, message: &str);
}

impl<T> ResultAsserts for Result<T> {
    fn assert_error(&self, message: &str) {
        let Err(err) = self else {
            panic!("Expected error");
        };
        assert!(err.to_string().contains(message), "unexpected error: {err}");
    }
}

macro_rules! btree_map {
    () => {
        ::std::collections::BTreeMap::new()
    };
    ($($key:expr => $value:expr),* $(,)?) => {
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
    ($($key:expr => $value:expr),* $(,)?) => {
        {
            let mut m = ::std::collections::HashMap::new();
            $(m.insert($key.into(), $value.into());)*
            m
        }
    };
}

pub(crate) use hash_map;
