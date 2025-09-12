//! Support for tests
use core::str;

use marrow::array::{Array, BytesArray};

use crate::internal::error::{fail, Error, Result};

pub fn assert_error_contains<T, E: std::fmt::Display>(actual: &Result<T, E>, expected: &str) {
    let Err(actual) = actual else {
        panic!("Expected an error, but no error was raised");
    };

    let actual = actual.to_string();
    if !actual.contains(expected) {
        panic!("Error did not contain {expected:?}. Full error: {actual}");
    }
}

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

#[allow(unused)]
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

#[allow(unused)]
pub(crate) use btree_map;

use super::utils::array_ext::get_bit_buffer;

#[allow(unused)]
pub(crate) trait ArrayAccess {
    fn get_utf8(&self, idx: usize) -> Result<Option<&str>>;
}

impl ArrayAccess for Array {
    fn get_utf8(&self, idx: usize) -> Result<Option<&str>> {
        match self {
            Self::Binary(array) | Self::Utf8(array) => get_utf8_impl(array, idx),
            Self::LargeBinary(array) | Self::LargeUtf8(array) => get_utf8_impl(array, idx),
            _ => fail!("invalid array type. does not support `get_utf8`"),
        }
    }
}

#[allow(unused)]
fn get_utf8_impl<O>(array: &BytesArray<O>, idx: usize) -> Result<Option<&str>>
where
    O: Copy,
    usize: TryFrom<O>,
    Error: From<<usize as TryFrom<O>>::Error>,
{
    if let Some(validity) = array.validity.as_ref() {
        if !get_bit_buffer(validity, 0, idx)? {
            return Ok(None);
        }
    }

    let Some(start) = array.offsets.get(idx) else {
        fail!("Could not get start for element {idx}");
    };
    let Some(end) = array.offsets.get(idx + 1) else {
        fail!("Could not get end for element {idx}");
    };

    let start = usize::try_from(*start)?;
    let end = usize::try_from(*end)?;
    let Some(data) = array.data.get(start..end) else {
        fail!("Invalid array. Could not get byte slice");
    };

    Ok(Some(str::from_utf8(data)?))
}
