use std::time::Duration;

use arrow2::{
    array::{Array, ListArray, MutableArray, MutablePrimitiveArray, PrimitiveArray},
    datatypes::{DataType, Field},
};
use criterion::{black_box, criterion_group, criterion_main, Criterion};

use rand::{
    distributions::{Standard, Uniform},
    prelude::Distribution,
    Rng,
};
use serde::{Deserialize, Serialize};
use serde_arrow::{
    arrow2::{deserialize_from_arrays, serialize_into_arrays, serialize_into_fields},
    base::{deserialize_from_source, serialize_into_sink, Event, EventSink},
};

// a sink that avoids allocations to "own" strings
#[derive(Default)]
struct BenchmarkSink(Vec<Event<'static>>);

impl EventSink for BenchmarkSink {
    fn accept(&mut self, event: Event<'_>) -> serde_arrow::Result<()> {
        self.0.push(match event {
            Event::Str(_) => Event::Str(""),
            Event::OwnedStr(_) => Event::Str(""),
            Event::Variant(_, idx) => Event::Variant("", idx),
            Event::OwnedVariant(_, idx) => Event::Variant("", idx),
            ev => ev.to_static(),
        });
        Ok(())
    }
}

fn criterion_benchmark(c: &mut Criterion) {
    let mut rng = rand::thread_rng();

    let items = (0..100_000)
        .map(|_| Item::random(&mut rng))
        .collect::<Vec<_>>();
    let fields = serialize_into_fields(&items).unwrap();
    let arrays = serialize_into_arrays(&fields, &items).unwrap();

    let mut events_for_deserialization = Vec::new();
    serialize_into_sink(&mut events_for_deserialization, &items).unwrap();

    let bincode_for_deserialization = bincode::serialize(&items).unwrap();

    let mut group = c.benchmark_group("benches");
    group.sample_size(20);
    group.measurement_time(Duration::from_secs(60));

    group.bench_function("serialize_into_sink", |b| {
        b.iter(|| {
            let mut events = BenchmarkSink::default();
            serialize_into_sink(&mut events, &items).unwrap();
            black_box(events)
        });
    });

    group.bench_function("deserialize_from_source", |b| {
        b.iter(|| {
            black_box::<Vec<Item>>(deserialize_from_source(&events_for_deserialization).unwrap())
        });
    });

    group.bench_function("serialize_into_bincode", |b| {
        b.iter(|| {
            let encoded = bincode::serialize(&items).unwrap();
            black_box(encoded);
        })
    });

    group.bench_function("deserialize_from_bincode", |b| {
        b.iter(|| {
            let decoded = bincode::serialize(&bincode_for_deserialization).unwrap();
            black_box(decoded);
        })
    });

    group.bench_function("serialize_into_arrays", |b| {
        b.iter(|| black_box(serialize_into_arrays(&fields, &items).unwrap()));
    });

    group.bench_function("deserialize_from_arrays", |b| {
        b.iter(|| black_box::<Vec<Item>>(deserialize_from_arrays(&fields, &arrays).unwrap()));
    });

    group.bench_function("manually_serialize", |b| {
        b.iter(|| {
            let mut a = MutablePrimitiveArray::<u16>::new();
            let mut b = MutablePrimitiveArray::<u32>::new();
            let mut c = MutablePrimitiveArray::<u64>::new();
            let mut d = MutablePrimitiveArray::<f64>::new();
            let mut offsets: Vec<i64> = vec![0];

            for item in &items {
                a.push(Some(item.a));
                b.push(Some(item.b));
                c.push(Some(item.c));
                for &dd in &item.d {
                    d.push(Some(dd));
                }
                offsets.push(d.len() as i64);
            }

            let arrays: Vec<Box<dyn Array>> = vec![
                Box::new(PrimitiveArray::from(a)),
                Box::new(PrimitiveArray::from(b)),
                Box::new(PrimitiveArray::from(c)),
                Box::new(ListArray::new(
                    DataType::LargeList(Box::new(Field::new("item", DataType::Float64, false))),
                    offsets.into(),
                    Box::new(PrimitiveArray::from(d)),
                    None,
                )),
            ];
            black_box(arrays);
        })
    });

    group.finish();
}

#[derive(Debug, Serialize, Deserialize)]
struct Item {
    a: u16,
    b: u32,
    c: u64,
    d: Vec<f64>,
}

impl Item {
    fn random<R: Rng + ?Sized>(rng: &mut R) -> Self {
        let len: usize = Uniform::new(1, 100).sample(rng);
        Self {
            a: Standard.sample(rng),
            b: Standard.sample(rng),
            c: Standard.sample(rng),
            d: (0..len).map(|_| Standard.sample(rng)).collect(),
        }
    }
}

criterion_group!(benches, criterion_benchmark);
criterion_main!(benches);
