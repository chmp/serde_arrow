use serde::Serialize;
use serde_arrow::marrow::datatypes::{DataType, Field};

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
        Field {
            name: String::from("i64"),
            data_type: DataType::Int64,
            ..Default::default()
        },
        Field {
            name: String::from("f32"),
            data_type: DataType::Float32,
            ..Default::default()
        },
        Field {
            name: String::from("str"),
            data_type: DataType::LargeUtf8,
            ..Default::default()
        },
    ];

    let items = build_example_data();
    for _ in 0..NUM_ITERATIONS {
        let arrays = serde_arrow::to_marrow(&fields, &items).unwrap();
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
