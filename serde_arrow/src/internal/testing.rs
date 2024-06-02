//! Support for tests
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
