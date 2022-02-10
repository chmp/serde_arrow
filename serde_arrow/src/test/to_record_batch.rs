use serde::Serialize;

use crate::{
    ng::{to_record_batch, trace_schema},
    Result,
};

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
