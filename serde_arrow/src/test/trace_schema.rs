use crate::{ng::trace_schema, DataType, Result, Schema};

use serde::Serialize;

#[test]
fn example() -> Result<()> {
    #[derive(Serialize)]
    struct Example {
        a: i8,
        b: i32,
    }

    let mut expected = Schema::new();
    expected.add_field("a", Some(DataType::I8), Some(false));
    expected.add_field("b", Some(DataType::I32), Some(false));

    let rows = &[Example { a: 0, b: 0 }];
    let actual = trace_schema(&rows)?;

    assert_eq!(actual, expected);

    Ok(())
}
