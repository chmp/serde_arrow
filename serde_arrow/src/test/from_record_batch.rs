use crate::{
    event::{Event, RecordBatchSource},
    from_record_batch, DataType, Result,
};

use arrow::datatypes::DataType as ArrowDataType;

use serde::{Deserialize, Serialize};

#[test]
fn event_source() -> Result<()> {
    #[derive(Serialize, Deserialize, PartialEq, Eq, Debug)]
    struct Example {
        int8: i8,
        int32: i32,
    }

    let original = &[
        Example { int8: 0, int32: 21 },
        Example { int8: 1, int32: 42 },
    ];
    let schema = crate::trace_schema(&original)?;
    let record_batch = crate::to_record_batch(&original, &schema)?;

    let mut event_source = RecordBatchSource::new(&record_batch, &schema)?;

    assert_eq!(event_source.next(), Event::StartSequence);
    assert_eq!(event_source.next(), Event::StartMap);
    assert_eq!(event_source.next(), Event::Key("int8"));
    assert_eq!(event_source.next(), Event::I8(0));
    assert_eq!(event_source.next(), Event::Key("int32"));
    assert_eq!(event_source.next(), Event::I32(21));
    assert_eq!(event_source.next(), Event::EndMap);
    assert_eq!(event_source.next(), Event::StartMap);
    assert_eq!(event_source.next(), Event::Key("int8"));
    assert_eq!(event_source.next(), Event::I8(1));
    assert_eq!(event_source.next(), Event::Key("int32"));
    assert_eq!(event_source.next(), Event::I32(42));
    assert_eq!(event_source.next(), Event::EndMap);
    assert_eq!(event_source.next(), Event::EndSequence);

    Ok(())
}

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
    let schema = crate::trace_schema(&original)?;
    let record_batch = crate::to_record_batch(&original, &schema)?;
    let round_tripped = from_record_batch::<Vec<Example>>(&record_batch, &schema)?;

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
    let schema = crate::trace_schema(&original)?;
    let record_batch = crate::to_record_batch(&original, &schema)?;
    let round_tripped = from_record_batch::<Vec<Example>>(&record_batch, &schema)?;

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
    let schema = crate::trace_schema(&original)?;
    let record_batch = crate::to_record_batch(&original, &schema)?;
    let round_tripped = from_record_batch::<Vec<Example>>(&record_batch, &schema)?;

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
    let schema = crate::trace_schema(&original)?.with_field(
        "val",
        Some(DataType::Arrow(ArrowDataType::LargeUtf8)),
        None,
    );
    let record_batch = crate::to_record_batch(&original, &schema)?;
    let round_tripped = from_record_batch::<Vec<Example>>(&record_batch, &schema)?;

    assert_eq!(round_tripped, original);

    Ok(())
}
