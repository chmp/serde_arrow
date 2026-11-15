//! Compare with and without `--features serde_arrow/unsafe-optimizations`.
//! Input setup and output destruction are excluded from timing.

use std::{hint::black_box, sync::Arc};

use arrow_schema::{DataType, Field};
use criterion::{BatchSize, Criterion, Throughput};
use serde::{Deserialize, Serialize};

const ROWS: usize = 1024;

#[derive(Deserialize, Serialize)]
struct Row {
    value: String,
}

fn benchmark_serialize(c: &mut Criterion) {
    let mut group = super::new_group(c, "utf8_validation");
    // Multiples of 12 keep the byte counts exact for every pattern.
    for (name, data_type, pattern, bytes) in [
        ("utf8_ascii_12b", DataType::Utf8, "a", 12),
        ("utf8_ascii_4092b", DataType::Utf8, "a", 4092),
        ("utf8_chinese_12b", DataType::Utf8, "中", 12),
        ("utf8_chinese_4092b", DataType::Utf8, "中", 4092),
        ("utf8_emoji_4092b", DataType::Utf8, "🦀", 4092),
        ("large_utf8_chinese_4092b", DataType::LargeUtf8, "中", 4092),
        ("utf8_view_chinese_12b", DataType::Utf8View, "中", 12),
        ("utf8_view_chinese_4092b", DataType::Utf8View, "中", 4092),
    ] {
        let fields = vec![Arc::new(Field::new("value", data_type, false))];
        let items = (0..ROWS)
            .map(|_| Row {
                value: pattern.repeat(bytes / pattern.len()),
            })
            .collect::<Vec<_>>();
        let batch = serde_arrow::to_record_batch(&fields, &items).unwrap();
        for column in batch.columns() {
            column.to_data().validate_full().unwrap();
        }

        group.throughput(Throughput::Bytes((ROWS * bytes) as u64));
        group.bench_function(format!("to_record_batch_{name}"), |b| {
            b.iter_batched(
                || (),
                |()| serde_arrow::to_record_batch(black_box(&fields), black_box(&items)).unwrap(),
                BatchSize::PerIteration,
            );
        });
        group.bench_function(format!("from_record_batch_{name}"), |b| {
            b.iter(|| serde_arrow::from_record_batch::<Vec<Row>>(black_box(&batch)).unwrap());
        });
    }
    group.finish();
}

criterion::criterion_group!(benchmark, benchmark_serialize);
