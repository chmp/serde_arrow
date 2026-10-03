use criterion::Criterion;
use serde::{Deserialize, Serialize};
use serde_arrow::{
    marrow::datatypes::{DataType, Field},
    to_marrow,
};

const VALUE_LEN: usize = 1_024;

#[derive(Debug, PartialEq, Serialize, Deserialize)]
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

pub fn benchmark_deserialize(c: &mut Criterion) {
    let mut group = super::new_group(c, "binary_values_1000_deserialize");
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
        let arrays = to_marrow(&fields, &items).unwrap();
        let views = arrays
            .iter()
            .map(|array| array.as_view())
            .collect::<Vec<_>>();
        let decoded: Vec<Item> = serde_arrow::from_marrow(&fields, &views).unwrap();
        assert_eq!(decoded, items);
        group.bench_function(name, |b| {
            b.iter(|| {
                let decoded: Vec<Item> = serde_arrow::from_marrow(&fields, &views).unwrap();
                criterion::black_box(decoded)
            })
        });
    }

    group.finish();
}

criterion::criterion_group!(benchmark, benchmark_serialize, benchmark_deserialize);
