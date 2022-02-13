use crate::{DataType, Result, Schema};

use serde::Serialize;

#[test]
fn example() -> Result<()> {
    #[derive(Serialize)]
    struct Example {
        a: i8,
        b: i32,
    }

    let expected = Schema::new()
        .with_field("a", Some(DataType::I8), Some(false))
        .with_field("b", Some(DataType::I32), Some(false));

    let rows = &[Example { a: 0, b: 0 }];
    let actual = Schema::from_records(&rows)?;

    assert_eq!(actual, expected);

    Ok(())
}

#[test]
fn example_option() -> Result<()> {
    #[derive(Serialize)]
    struct Example {
        a: Option<u8>,
    }

    let expected = Schema::new().with_field("a", Some(DataType::U8), Some(true));

    let actual = Schema::from_records(&&[Example { a: Some(0) }])?;
    assert_eq!(actual, expected);

    let actual = Schema::from_records(&&[Example { a: Some(0) }, Example { a: None }])?;
    assert_eq!(actual, expected);

    Ok(())
}
