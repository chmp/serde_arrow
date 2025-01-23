//! Support for tests
use core::str;
use std::collections::HashMap;

use crate::internal::{
    arrow::{Array, BytesArray, DataType, Field},
    error::{fail, Error, Result},
};
use crate::schema::{Strategy, STRATEGY_KEY};

use serde::{Deserialize, Serialize};

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

use super::utils::array_ext::get_bit_buffer;

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

fn enum_with_named_fields_metadata() -> HashMap<String, String> {
    HashMap::from([(
        STRATEGY_KEY.to_string(),
        Strategy::EnumsWithNamedFieldsAsStructs.to_string(),
    )])
}

// Simple enum test structure for schema from_type/from_samples unit testing
#[derive(Serialize, Deserialize)]
pub(crate) enum Number {
    Real { value: f32 },
    Complex { i: f32, j: f32 },
}

impl Number {
    pub(crate) fn sample_items() -> Vec<Self> {
        vec![
            Number::Real { value: 1.0 },
            Number::Complex { i: 0.5, j: 0.5 },
        ]
    }

    pub(crate) fn expected_field() -> Field {
        Field {
            name: "$".to_string(),
            data_type: DataType::Struct(vec![
                Field {
                    name: "Complex::i".to_string(),
                    data_type: DataType::Float32,
                    nullable: true,
                    metadata: HashMap::new(),
                },
                Field {
                    name: "Complex::j".to_string(),
                    data_type: DataType::Float32,
                    nullable: true,
                    metadata: HashMap::new(),
                },
                Field {
                    name: "Real::value".to_string(),
                    data_type: DataType::Float32,
                    nullable: true,
                    metadata: HashMap::new(),
                },
            ]),
            nullable: false,
            metadata: enum_with_named_fields_metadata(),
        }
    }
}

// No data test enum
#[derive(Serialize, Deserialize)]
pub(crate) enum Coin {
    Heads,
    Tails,
}

impl Coin {
    pub(crate) fn sample_items() -> Vec<Self> {
        vec![Coin::Heads, Coin::Tails]
    }

    pub(crate) fn expected_field() -> Field {
        Field {
            name: "$".to_string(),
            data_type: DataType::Dictionary(
                Box::new(DataType::UInt32),
                Box::new(DataType::LargeUtf8),
                false,
            ),
            nullable: false,
            metadata: HashMap::new(),
        }
    }
}

// Optional variant field test enum
#[derive(Serialize, Deserialize)]
pub(crate) enum Optionals {
    Something {
        more: Option<usize>,
        less: Option<usize>,
    },
    Else {
        one: Option<usize>,
        another: Option<usize>,
    },
}

impl Optionals {
    pub(crate) fn sample_items() -> Vec<Self> {
        vec![
            Optionals::Something {
                more: Some(1),
                less: None,
            },
            Optionals::Something {
                more: None,
                less: Some(0),
            },
            Optionals::Else {
                one: None,
                another: Some(0),
            },
            Optionals::Else {
                one: Some(1),
                another: None,
            },
        ]
    }

    pub(crate) fn expected_field() -> Field {
        Field {
            name: "$".to_string(),
            data_type: DataType::Struct(vec![
                Field {
                    name: "Else::another".to_string(),
                    data_type: DataType::UInt64,
                    nullable: true,
                    metadata: HashMap::new(),
                },
                Field {
                    name: "Else::one".to_string(),
                    data_type: DataType::UInt64,
                    nullable: true,
                    metadata: HashMap::new(),
                },
                Field {
                    name: "Something::less".to_string(),
                    data_type: DataType::UInt64,
                    nullable: true,
                    metadata: HashMap::new(),
                },
                Field {
                    name: "Something::more".to_string(),
                    data_type: DataType::UInt64,
                    nullable: true,
                    metadata: HashMap::new(),
                },
            ]),
            nullable: false,
            metadata: enum_with_named_fields_metadata(),
        }
    }
}

// Tuple variant test enum
#[derive(Serialize, Deserialize)]
pub(crate) enum Payment {
    Cash(f32),                                 // amount
    Check(String, f32),                        // name, amount
    CreditCard(String, f32, [u8; 16], String), // name, amount, cc number, exp
}

impl Payment {
    pub(crate) fn sample_items() -> Vec<Self> {
        vec![
            Payment::Cash(0.42),
            Payment::Check("Bob".to_string(), 0.42),
            Payment::CreditCard(
                "Sue".to_string(),
                0.42,
                [1, 2, 3, 4, 5, 6, 7, 8, 9, 0, 1, 2, 3, 4, 5, 6],
                "01/2024".to_string(),
            ),
        ]
    }
}
