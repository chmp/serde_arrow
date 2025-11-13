//! requires on of the `arrow-*` features
use std::sync::Arc;

use serde::Serialize;
use serde_arrow::_impl::arrow::datatypes::{DataType, Field};

const NUM_RECORDS: usize = 100;
const NUM_ITERATIONS: usize = 100_000;

#[derive(Serialize)]
struct Record {
    i64: i64,
    f32: f32,
    str: String,
}

fn main() {
    let fields = vec![
        Arc::new(Field::new("i64", DataType::Int64, false)),
        Arc::new(Field::new("f32", DataType::Float32, false)),
        Arc::new(Field::new("str", DataType::LargeUtf8, false)),
    ];

    let items = build_example_data();
    for _ in 0..NUM_ITERATIONS {
        let arrays = serde_arrow::to_arrow(&fields, &items).unwrap();
        let _ = criterion::black_box(arrays);
    }
}

fn build_example_data() -> Vec<Record> {
    let mut result = Vec::with_capacity(NUM_RECORDS);
    for i in 0..NUM_RECORDS {
        result.push(Record {
            i64: i64::try_from(i * i).unwrap(),
            f32: 1.0 / (1.0 + i as f32),
            str: format!("Item {i}"),
        });
    }
    result
}
