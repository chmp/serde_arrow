use criterion::Criterion;
use serde::Serialize;
use serde_arrow::{
    marrow::datatypes::{DataType, Field},
    to_marrow,
};

const VALUE_LEN: usize = 1_024;

#[derive(Serialize)]
struct Item {
    value: Vec<u8>,
}

pub fn benchmark_serialize(c: &mut Criterion) {
    let mut group = super::new_group(c, "binary_values_1000");
    let items = (0..1_000)
        .map(|idx| Item {
            value: (0..VALUE_LEN).map(|byte| (idx + byte) as u8).collect(),
        })
        .collect::<Vec<_>>();

    for (name, data_type) in [
        ("binary", DataType::Binary),
        ("large_binary", DataType::LargeBinary),
        ("binary_view", DataType::BinaryView),
    ] {
        let fields = vec![Field {
            name: "value".into(),
            data_type,
            nullable: false,
            metadata: Default::default(),
        }];
        group.bench_function(name, |b| {
            b.iter(|| criterion::black_box(to_marrow(&fields, &items).unwrap()))
        });
    }

    group.finish();
}

criterion::criterion_group!(benchmark, benchmark_serialize);
