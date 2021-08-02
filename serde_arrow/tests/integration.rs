use std::{collections::HashMap, convert::TryFrom};

use arrow::datatypes::{DataType, Schema};
use chrono::NaiveDateTime;
use serde::Serialize;

use serde_arrow::Result;

macro_rules! hashmap {
    () => {
        ::std::collections::HashMap::new()
    };
    ($($key:expr => $value:expr),*) => {
        {
            let mut m = ::std::collections::HashMap::new();
            $(m.insert($key.into(), $value.into());)*
            m
        }
    };
}

#[test]
fn example() -> Result<()> {
    #[derive(Serialize)]
    struct Example {
        int8: i8,
        int32: i32,
        float32: f32,
        date64: NaiveDateTime,
        boolean: bool,
    }

    let examples = vec![
        Example {
            float32: 1.0,
            int8: 1,
            int32: 4,
            date64: NaiveDateTime::from_timestamp(0, 0),
            boolean: true,
        },
        Example {
            float32: 2.0,
            int8: 2,
            int32: 5,
            date64: NaiveDateTime::from_timestamp(5 * 24 * 60 * 60, 0),
            boolean: false,
        },
    ];

    let mut schema = serde_arrow::trace_schema(&examples)?;
    schema.set_data_type("date", DataType::Date64);

    let schema = Schema::try_from(schema)?;

    serde_arrow::to_record_batch(&examples, schema)?;

    Ok(())
}

#[test]
fn example_maps() -> Result<()> {
    let examples: Vec<HashMap<String, i32>> = vec![
        hashmap! { "a" => 42, "b" => 32 },
        hashmap! { "a" => 42, "b" => 32 },
    ];

    let schema = serde_arrow::trace_schema(&examples)?;
    let schema = Schema::try_from(schema)?;

    serde_arrow::to_record_batch(&examples, schema)?;

    Ok(())
}

#[test]
fn example_flatten() -> Result<()> {
    #[derive(Serialize)]
    struct Example {
        int8: i8,
        int32: i32,

        #[serde(flatten)]
        extra: HashMap<String, i32>,
    }

    let examples = vec![
        Example {
            int8: 1,
            int32: 4,
            extra: hashmap! { "a" => 2 },
        },
        Example {
            int8: 2,
            int32: 5,
            extra: hashmap! { "a" => 3 },
        },
    ];

    let mut schema = serde_arrow::trace_schema(&examples)?;
    schema.set_data_type("date", DataType::Date64);

    let schema = Schema::try_from(schema)?;

    serde_arrow::to_record_batch(&examples, schema)?;

    Ok(())
}

macro_rules! define_api_test {
    (#[ignore] $name:ident, rows = $rows:expr) => {
        #[ignore]
        #[test]
        fn $name() -> Result<()> {
            define_api_test!(__body__; $rows)
        }
    };
    ($name:ident, rows = $rows:expr) => {
        #[test]
        fn $name() -> Result<()> {
            define_api_test!(__body__; $rows)
        }
    };
    (__body__; $rows:expr) => {
        {
            let rows = $rows;
            let schema = serde_arrow::trace_schema(rows)?;
            let schema = Schema::try_from(schema)?;
            serde_arrow::to_record_batch(rows, schema)?;

            Ok(())
        }
    };
}

#[derive(Serialize)]
struct Record {
    val: i8,
}

define_api_test!(
    api_serialize_slice,
    rows = {
        let rows = &[Record { val: 1 }, Record { val: 2 }];
        &rows[..]
    }
);

// currently not supported
define_api_test!(
    #[ignore]
    api_serialize_fixed_array,
    rows = &[Record { val: 1 }, Record { val: 2 }]
);

// currently not supported
define_api_test!(
    #[ignore]
    api_serialize_tuple,
    rows = &(Record { val: 1 }, Record { val: 2 })
);
