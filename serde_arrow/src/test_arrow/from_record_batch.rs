use crate::{DataType, Result, Schema};

use arrow::datatypes::DataType as ArrowDataType;

use serde::{Deserialize, Serialize};

#[test]
fn example() -> Result<()> {
    #[derive(Serialize, Deserialize, PartialEq, Eq, Debug)]
    struct Example {
        int8: i8,
        int32: i32,
    }

    let original = &[
        Example { int8: 0, int32: 21 },
        Example { int8: 1, int32: 42 },
    ];
    let schema = Schema::from_records(&original)?;
    let record_batch = crate::arrow::to_record_batch(&original, &schema)?;
    let round_tripped = crate::arrow::from_record_batch::<Vec<Example>>(&record_batch, &schema)?;

    assert_eq!(round_tripped, original);

    Ok(())
}

#[test]
fn example_nullable() -> Result<()> {
    #[derive(Serialize, Deserialize, PartialEq, Eq, Debug)]
    struct Example {
        val: Option<u32>,
    }

    let original = &[
        Example { val: Some(21) },
        Example { val: None },
        Example { val: Some(42) },
    ];
    let schema = Schema::from_records(&original)?;
    let record_batch = crate::arrow::to_record_batch(&original, &schema)?;
    let round_tripped = crate::arrow::from_record_batch::<Vec<Example>>(&record_batch, &schema)?;

    assert_eq!(round_tripped, original);

    Ok(())
}

#[test]
fn example_string() -> Result<()> {
    #[derive(Serialize, Deserialize, PartialEq, Eq, Debug)]
    struct Example {
        val: String,
    }

    let original = &[
        Example {
            val: String::from("foo"),
        },
        Example {
            val: String::from("bar"),
        },
        Example {
            val: String::from("baz"),
        },
    ];
    let schema = Schema::from_records(&original)?;
    let record_batch = crate::arrow::to_record_batch(&original, &schema)?;
    let round_tripped = crate::arrow::from_record_batch::<Vec<Example>>(&record_batch, &schema)?;

    assert_eq!(round_tripped, original);

    Ok(())
}

#[test]
fn example_large_string() -> Result<()> {
    #[derive(Serialize, Deserialize, PartialEq, Eq, Debug)]
    struct Example {
        val: String,
    }

    let original = &[
        Example {
            val: String::from("foo"),
        },
        Example {
            val: String::from("bar"),
        },
        Example {
            val: String::from("baz"),
        },
    ];
    let schema = Schema::from_records(&original)?.with_field(
        "val",
        Some(DataType::Arrow(ArrowDataType::LargeUtf8)),
        None,
    );
    let record_batch = crate::arrow::to_record_batch(&original, &schema)?;
    let round_tripped = crate::arrow::from_record_batch::<Vec<Example>>(&record_batch, &schema)?;

    assert_eq!(round_tripped, original);

    Ok(())
}

#[test]
fn example_char() -> Result<()> {
    #[derive(Serialize, Deserialize, PartialEq, Eq, Debug)]
    struct Example {
        val: char,
    }

    let original = &[
        Example { val: 'f' },
        Example { val: 'o' },
        Example { val: 'o' },
    ];
    let schema = Schema::from_records(&original)?;
    let record_batch = crate::arrow::to_record_batch(&original, &schema)?;
    let round_tripped = crate::arrow::from_record_batch::<Vec<Example>>(&record_batch, &schema)?;

    assert_eq!(round_tripped, original);

    Ok(())
}
