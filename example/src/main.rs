use std::{collections::HashMap, fs::File};

use chrono::NaiveDateTime;
use serde::Serialize;

use arrow::datatypes::FieldRef;
use serde_arrow::schema::ext::FixedShapeTensorField;
use serde_json::json;

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
    r#type: SampleType,
    tensor: Vec<i32>,
    int8: i8,
    int32: i32,
    float32: f32,
    date64: NaiveDateTime,
    boolean: bool,
    map: HashMap<String, i32>,
    nested: Nested,
}

#[derive(Serialize)]
enum SampleType {
    A,
    B,
    C,
}

#[derive(Serialize)]
struct Nested {
    a: Option<f32>,
    b: Nested2,
}

#[derive(Serialize)]
struct Nested2 {
    foo: String,
}

#[allow(deprecated)]
fn main() -> Result<(), PanicOnError> {
    let examples = vec![
        Example {
            r#type: SampleType::A,
            tensor: vec![1, 2, 3, 4],
            float32: 1.0,
            int8: 1,
            int32: 4,
            date64: NaiveDateTime::from_timestamp(0, 0),
            boolean: true,
            map: hashmap! { "a" => 2 },
            nested: Nested {
                a: Some(42.0),
                b: Nested2 {
                    foo: String::from("hello"),
                },
            },
        },
        Example {
            r#type: SampleType::B,
            tensor: vec![4, 5, 6, 7],
            float32: 2.0,
            int8: 2,
            int32: 5,
            date64: NaiveDateTime::from_timestamp(5 * 24 * 60 * 60, 0),
            boolean: false,
            map: hashmap! { "a" => 3 },
            nested: Nested {
                a: None,
                b: Nested2 {
                    foo: String::from("world"),
                },
            },
        },
        Example {
            r#type: SampleType::C,
            tensor: vec![8, 9, 10, 11],
            float32: 12.0,
            int8: -5,
            int32: 50,
            date64: NaiveDateTime::from_timestamp(5 * 24 * 60 * 60, 0),
            boolean: true,
            map: hashmap! { "a" => 3, "b" => 4 },
            nested: Nested {
                a: Some(2.0),
                b: Nested2 {
                    foo: String::from("world"),
                },
            },
        },
    ];

    use serde_arrow::schema::{SchemaLike, TracingOptions};

    let tracing_options = TracingOptions::default()
        .guess_dates(true)
        .enums_without_data_as_strings(true)
        .overwrite(
            "tensor",
            FixedShapeTensorField::new(
                "tensor",
                json!({"name": "element", "data_type": "I32"}),
                [2, 2],
            )?,
        )?;

    let fields = Vec::<FieldRef>::from_samples(&examples, tracing_options)?;
    let batch = serde_arrow::to_record_batch(&fields, &examples)?;

    let file = File::create("example.ipc")?;

    let mut writer = arrow::ipc::writer::FileWriter::try_new(file, &batch.schema())?;
    writer.write(&batch)?;
    writer.finish()?;

    Ok(())
}

#[derive(Debug)]
struct PanicOnError;

impl<E: std::fmt::Display> From<E> for PanicOnError {
    fn from(e: E) -> Self {
        panic!("Encountered error: {}", e);
    }
}
