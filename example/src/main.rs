use std::{collections::HashMap, convert::TryInto, fs::File, path::Path};

use chrono::NaiveDateTime;
use serde::Serialize;

use arrow2::{
    array::Array,
    chunk::Chunk,
    datatypes::{Field, Schema},
    io::ipc::write,
};

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
    map: HashMap<String, i32>,
    nested: Nested,
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
    ];

    use serde_arrow::schema::{SchemaLike, SerdeArrowSchema, TracingOptions};

    let fields: Vec<Field> =
        SerdeArrowSchema::from_samples(&examples, TracingOptions::default().guess_dates(true))?
            .try_into()?;
    let arrays = serde_arrow::to_arrow2(&fields, &examples)?;

    let schema = Schema::from(fields);
    let chunk = Chunk::new(arrays);

    write_batches("example.ipc", schema, &[chunk])?;

    Ok(())
}

fn write_batches<P: AsRef<Path>>(
    path: P,
    schema: Schema,
    chunks: &[Chunk<Box<dyn Array>>],
) -> Result<(), PanicOnError> {
    let file = File::create(path)?;

    let options = write::WriteOptions { compression: None };
    let mut writer = write::FileWriter::new(file, schema, None, options);

    writer.start()?;
    for chunk in chunks {
        writer.write(chunk, None)?
    }
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
