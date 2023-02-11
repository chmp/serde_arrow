use std::time::Duration;

use arrow2::{
    array::{
        Array, BooleanArray, MutableArray, MutableBooleanArray, MutablePrimitiveArray,
        MutableUtf8Array, PrimitiveArray, StructArray, UnionArray, Utf8Array,
    },
    buffer::Buffer,
    datatypes::{DataType, Field},
};
use criterion::{black_box, criterion_group, criterion_main, Criterion};

use rand::{
    distributions::{Standard, Uniform},
    prelude::Distribution,
    Rng,
};
use serde::{Deserialize, Serialize};
use serde_arrow::arrow2::{deserialize_from_arrays, serialize_into_arrays, serialize_into_fields};

fn benchmark_primitives(c: &mut Criterion) {
    let mut group = c.benchmark_group("primitives");
    group.sample_size(20);
    group.sampling_mode(criterion::SamplingMode::Flat);
    group.measurement_time(Duration::from_secs(60));

    #[derive(Debug, Serialize, Deserialize)]
    struct Item {
        a: u8,
        b: u16,
        c: u32,
        d: u64,
        e: i8,
        f: i16,
        g: i32,
        h: i64,
        i: f32,
        j: f64,
        k: bool,
    }

    impl Item {
        fn random<R: Rng + ?Sized>(rng: &mut R) -> Self {
            Self {
                a: Standard.sample(rng),
                b: Standard.sample(rng),
                c: Standard.sample(rng),
                d: Standard.sample(rng),
                e: Standard.sample(rng),
                f: Standard.sample(rng),
                g: Standard.sample(rng),
                h: Standard.sample(rng),
                i: Standard.sample(rng),
                j: Standard.sample(rng),
                k: Standard.sample(rng),
            }
        }
    }

    let mut rng = rand::thread_rng();

    let items = (0..100_000)
        .map(|_| Item::random(&mut rng))
        .collect::<Vec<_>>();
    let fields = serialize_into_fields(&items).unwrap();
    let arrays = serialize_into_arrays(&fields, &items).unwrap();

    group.bench_function("serialize_into_arrays", |b| {
        b.iter(|| black_box(serialize_into_arrays(&fields, &items).unwrap()));
    });

    group.bench_function("deserialize_from_arrays", |b| {
        b.iter(|| black_box::<Vec<Item>>(deserialize_from_arrays(&fields, &arrays).unwrap()));
    });

    group.bench_function("manually_serialize", |b| {
        b.iter(|| {
            let mut a = MutablePrimitiveArray::<u8>::new();
            let mut b = MutablePrimitiveArray::<u16>::new();
            let mut c = MutablePrimitiveArray::<u32>::new();
            let mut d = MutablePrimitiveArray::<u64>::new();
            let mut e = MutablePrimitiveArray::<i8>::new();
            let mut f = MutablePrimitiveArray::<i16>::new();
            let mut g = MutablePrimitiveArray::<i32>::new();
            let mut h = MutablePrimitiveArray::<i64>::new();
            let mut i = MutablePrimitiveArray::<f32>::new();
            let mut j = MutablePrimitiveArray::<f64>::new();
            let mut k = MutableBooleanArray::new();

            for item in &items {
                a.push(Some(item.a));
                b.push(Some(item.b));
                c.push(Some(item.c));
                d.push(Some(item.d));
                e.push(Some(item.e));
                f.push(Some(item.f));
                g.push(Some(item.g));
                h.push(Some(item.h));
                i.push(Some(item.i));
                j.push(Some(item.j));
                k.push(Some(item.k));
            }

            let arrays: Vec<Box<dyn Array>> = vec![
                Box::new(PrimitiveArray::from(a)),
                Box::new(PrimitiveArray::from(b)),
                Box::new(PrimitiveArray::from(c)),
                Box::new(PrimitiveArray::from(d)),
                Box::new(PrimitiveArray::from(e)),
                Box::new(PrimitiveArray::from(f)),
                Box::new(PrimitiveArray::from(g)),
                Box::new(PrimitiveArray::from(h)),
                Box::new(PrimitiveArray::from(i)),
                Box::new(PrimitiveArray::from(j)),
                Box::new(BooleanArray::from(k)),
            ];
            black_box(arrays);
        })
    });

    group.bench_function("manually_deserialize", |b| {
        b.iter(|| {
            let mut res = Vec::new();

            let a = arrays[0]
                .as_any()
                .downcast_ref::<PrimitiveArray<u8>>()
                .unwrap();
            let b = arrays[1]
                .as_any()
                .downcast_ref::<PrimitiveArray<u16>>()
                .unwrap();
            let c = arrays[2]
                .as_any()
                .downcast_ref::<PrimitiveArray<u32>>()
                .unwrap();
            let d = arrays[3]
                .as_any()
                .downcast_ref::<PrimitiveArray<u64>>()
                .unwrap();
            let e = arrays[4]
                .as_any()
                .downcast_ref::<PrimitiveArray<i8>>()
                .unwrap();
            let f = arrays[5]
                .as_any()
                .downcast_ref::<PrimitiveArray<i16>>()
                .unwrap();
            let g = arrays[6]
                .as_any()
                .downcast_ref::<PrimitiveArray<i32>>()
                .unwrap();
            let h = arrays[7]
                .as_any()
                .downcast_ref::<PrimitiveArray<i64>>()
                .unwrap();
            let i = arrays[8]
                .as_any()
                .downcast_ref::<PrimitiveArray<f32>>()
                .unwrap();
            let j = arrays[9]
                .as_any()
                .downcast_ref::<PrimitiveArray<f64>>()
                .unwrap();
            let k = arrays[10].as_any().downcast_ref::<BooleanArray>().unwrap();

            for ii in 0..a.len() {
                res.push(Item {
                    a: a.value(ii),
                    b: b.value(ii),
                    c: c.value(ii),
                    d: d.value(ii),
                    e: e.value(ii),
                    f: f.value(ii),
                    g: g.value(ii),
                    h: h.value(ii),
                    i: i.value(ii),
                    j: j.value(ii),
                    k: k.value(ii),
                });
            }

            black_box(res);
        })
    });

    group.finish();
}

fn benchmark_complex(c: &mut Criterion) {
    let mut group = c.benchmark_group("complex");
    group.sample_size(20);
    group.sampling_mode(criterion::SamplingMode::Flat);
    group.measurement_time(Duration::from_secs(60));

    #[derive(Debug, Serialize, Deserialize)]
    struct Item {
        string: String,
        points: Vec<(f32, f32)>,
        float: Float,
    }

    #[derive(Debug, Serialize, Deserialize)]
    enum Float {
        F32(f32),
        F64(f64),
    }

    impl Item {
        fn random<R: Rng + ?Sized>(rng: &mut R) -> Self {
            let n_string = Uniform::new(1, 50).sample(rng);
            let n_points = Uniform::new(1, 50).sample(rng);
            let is_f32: bool = Standard.sample(rng);

            Self {
                string: (0..n_string)
                    .map(|_| -> char { Standard.sample(rng) })
                    .collect(),
                points: (0..n_points)
                    .map(|_| (Standard.sample(rng), Standard.sample(rng)))
                    .collect(),
                float: if is_f32 {
                    Float::F32(Standard.sample(rng))
                } else {
                    Float::F64(Standard.sample(rng))
                },
            }
        }
    }

    let mut rng = rand::thread_rng();

    let items = (0..100_000)
        .map(|_| Item::random(&mut rng))
        .collect::<Vec<_>>();
    let fields = serialize_into_fields(&items).unwrap();
    let arrays = serialize_into_arrays(&fields, &items).unwrap();

    group.bench_function("serialize_into_arrays", |b| {
        b.iter(|| black_box(serialize_into_arrays(&fields, &items).unwrap()));
    });

    group.bench_function("deserialize_from_arrays", |b| {
        b.iter(|| black_box::<Vec<Item>>(deserialize_from_arrays(&fields, &arrays).unwrap()));
    });

    group.bench_function("manually_serialize", |b| {
        b.iter(|| {
            let mut string = MutableUtf8Array::<i64>::new();
            let mut points_0 = MutablePrimitiveArray::<f32>::new();
            let mut points_1 = MutablePrimitiveArray::<f32>::new();
            let mut points_offsets: Vec<i64> = vec![0];
            let mut floats_f32 = MutablePrimitiveArray::<f32>::new();
            let mut floats_f64 = MutablePrimitiveArray::<f64>::new();
            let mut floats_offsets: Vec<i32> = Vec::new();
            let mut floats_variant: Vec<i8> = Vec::new();

            for item in &items {
                string.push(Some(&item.string));
                for &point in &item.points {
                    points_0.push(Some(point.0));
                    points_1.push(Some(point.1));
                }
                points_offsets.push(points_0.len() as i64);

                match &item.float {
                    &Float::F32(val) => {
                        floats_offsets.push(floats_f32.len() as i32);
                        floats_f32.push(Some(val));
                        floats_variant.push(0);
                    }
                    &Float::F64(val) => {
                        floats_offsets.push(floats_f64.len() as i32);
                        floats_f64.push(Some(val));
                        floats_variant.push(1);
                    }
                }
            }

            let arrays: Vec<Box<dyn Array>> = vec![
                Box::new(<Utf8Array<_> as From<_>>::from(string)),
                Box::new(StructArray::new(
                    DataType::Struct(vec![
                        Field::new("0", DataType::Float32, false),
                        Field::new("1", DataType::Float32, false),
                    ]),
                    vec![
                        Box::new(PrimitiveArray::from(points_0)),
                        Box::new(PrimitiveArray::from(points_1)),
                    ],
                    None,
                )),
                Box::new(UnionArray::new(
                    DataType::Union(
                        vec![
                            Field::new("F32", DataType::Float32, false),
                            Field::new("F64", DataType::Float64, false),
                        ],
                        None,
                        arrow2::datatypes::UnionMode::Dense,
                    ),
                    Buffer::from(floats_variant),
                    vec![
                        Box::new(PrimitiveArray::from(floats_f32)),
                        Box::new(PrimitiveArray::from(floats_f64)),
                    ],
                    Some(Buffer::from(floats_offsets)),
                )),
            ];

            black_box(arrays);
        })
    });

    // TODO: implement
    // group.bench_function("manually_deserialize", |b| {
    //     b.iter(|| { })
    // });

    group.finish();
}

criterion_group!(benches, benchmark_primitives, benchmark_complex);
criterion_main!(benches);