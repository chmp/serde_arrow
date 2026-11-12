use arrow_array::{Array, ArrayRef, LargeStringArray};
use criterion::Criterion;
use serde::{Deserialize, Serialize};

#[derive(Debug, PartialEq, Deserialize, Serialize)]
struct Item {
    value: Option<String>,
}

pub fn benchmark_deserialize(c: &mut Criterion) {
    let items = (0..1_000)
        .map(|idx| Item {
            value: (idx % 10 != 0).then(|| format!("{idx:04}{}", "é".repeat(64))),
        })
        .collect::<Vec<_>>();
    let mut group = super::new_group(c, "nullable_strings_1000_deserialize");

    let arrow_fields = crate::impls::serde_arrow_arrow::trace(&items);
    let arrow_arrays = serde_arrow::to_arrow(&arrow_fields, &items).unwrap();
    assert_eq!(deserialize_manual(&arrow_arrays), items);
    group.bench_function("arrow_manual", |b| {
        b.iter(|| criterion::black_box(deserialize_manual(&arrow_arrays)))
    });
    let decoded: Vec<Item> = serde_arrow::from_arrow(&arrow_fields, &arrow_arrays).unwrap();
    assert_eq!(decoded, items);
    group.bench_function("serde_arrow_arrow", |b| {
        b.iter(|| {
            let decoded: Vec<Item> = serde_arrow::from_arrow(&arrow_fields, &arrow_arrays).unwrap();
            criterion::black_box(decoded)
        })
    });

    let marrow_fields = crate::impls::serde_arrow_marrow::trace(&items);
    let marrow_arrays = serde_arrow::to_marrow(&marrow_fields, &items).unwrap();
    let marrow_views = marrow_arrays
        .iter()
        .map(|array| array.as_view())
        .collect::<Vec<_>>();
    let decoded: Vec<Item> = serde_arrow::from_marrow(&marrow_fields, &marrow_views).unwrap();
    assert_eq!(decoded, items);
    group.bench_function("serde_arrow_marrow", |b| {
        b.iter(|| {
            let decoded: Vec<Item> =
                serde_arrow::from_marrow(&marrow_fields, &marrow_views).unwrap();
            criterion::black_box(decoded)
        })
    });

    group.finish();
}

fn deserialize_manual(arrays: &[ArrayRef]) -> Vec<Item> {
    let values = arrays[0]
        .as_any()
        .downcast_ref::<LargeStringArray>()
        .unwrap();
    (0..values.len())
        .map(|idx| Item {
            value: (!values.is_null(idx)).then(|| values.value(idx).to_owned()),
        })
        .collect()
}

criterion::criterion_group!(benchmark, benchmark_deserialize);
