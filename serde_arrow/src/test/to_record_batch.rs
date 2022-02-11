use serde::Serialize;

use crate::{to_record_batch, trace_schema, DataType, Result, Schema};

#[test]
fn example() -> Result<()> {
    #[derive(Serialize)]
    struct Example {
        a: i8,
        b: i32,
    }

    let rows = &[Example { a: 13, b: 21 }];
    let schema = trace_schema(&rows)?;
    let _actual = to_record_batch(&rows, &schema)?;

    // TODO: test

    Ok(())
}

#[test]
fn example_option() -> Result<()> {
    #[derive(Serialize)]
    struct Example {
        a: Option<u8>,
    }

    let schema = Schema::new().with_field("a", Some(DataType::U8), Some(true));

    let _actual = to_record_batch(&&[Example { a: Some(0) }], &schema)?;
    let _actual = to_record_batch(&&[Example { a: Some(0) }, Example { a: None }], &schema)?;

    Ok(())
}
