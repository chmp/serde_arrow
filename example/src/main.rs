use std::{collections::HashMap, convert::TryFrom};

use chrono::NaiveDateTime;
use serde::Serialize;
use serde_arrow::{DataType, Result, Schema};

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

#[derive(Serialize)]
struct Example {
    int8: i8,
    int32: i32,
    float32: f32,
    date64: NaiveDateTime,
    boolean: bool,
    #[serde(flatten)]
    extra: HashMap<String, i32>,
}

fn main() -> Result<()> {
    let examples = vec![
        Example {
            float32: 1.0,
            int8: 1,
            int32: 4,
            date64: NaiveDateTime::from_timestamp(0, 0),
            boolean: true,
            extra: hashmap! { "a" => 2 },
        },
        Example {
            float32: 2.0,
            int8: 2,
            int32: 5,
            date64: NaiveDateTime::from_timestamp(5 * 24 * 60 * 60, 0),
            boolean: false,
            extra: hashmap! { "a" => 3 },
        },
    ];

    let mut schema = Schema::from_records(&examples)?;
    schema.set_data_type("date64", DataType::NaiveDateTimeStr)?;

    {
        use arrow::csv;

        println!("# Arrow");
        println!();

        let batch = serde_arrow::arrow::to_record_batch(&examples, &schema)?;
        csv::Writer::new(std::io::stdout()).write(&batch)?;
    }
    {
        use polars::prelude::*;

        println!("# Polars");
        println!();

        let fields = schema.build_arrow2_fields()?;
        let chunk = serde_arrow::arrow2::to_chunk(&examples, &schema)?;

        let df = DataFrame::try_from((chunk, fields.as_slice())).unwrap();
        println!("{df}");
    }
    {
        println!("# Write IPC");
        println!();

        let mut data = Vec::new();
        serde_arrow::arrow2::write_ipc(&mut data, &examples, &schema)?;
        println!("{} bytes written", data.len());
    }

    Ok(())
}
