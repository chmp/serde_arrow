// TODO: clean up the bechmarks (split into files, rename ...)
use std::{sync::Arc, time::Duration};

use arrow_json_38::ReaderBuilder;
use arrow_schema_38::Schema;
use criterion::{black_box, criterion_group, criterion_main, Criterion};
use serde_arrow::_impl::arrow2::{
    array::{
        Array, BooleanArray, MutableArray, MutableBooleanArray, MutablePrimitiveArray,
        MutableUtf8Array, PrimitiveArray, StructArray, UnionArray, Utf8Array,
    },
    buffer::Buffer,
    datatypes::{DataType, Field, UnionMode},
};

use rand::{
    distributions::{Standard, Uniform},
    prelude::Distribution,
    Rng,
};
use serde::{Deserialize, Serialize};
use serde_arrow::{arrow, arrow2};

mod bytecode {
    use serde::Serialize;
    use serde_arrow::{
        Result,
        _impl::{
            arrow::array::ArrayRef,
            bytecode::{compile_serialization, CompilationOptions, Interpreter},
            GenericField,
        },
        base::serialize_into_sink,
        Error,
    };

    pub fn serialize<F, T>(fields: &[F], items: &T) -> Result<Vec<ArrayRef>>
    where
        GenericField: for<'a> TryFrom<&'a F, Error = Error>,
        T: Serialize + ?Sized,
    {
        let fields = fields
            .into_iter()
            .map(|f| f.try_into())
            .collect::<Result<Vec<GenericField>>>()?;
        let program = compile_serialization(&fields, CompilationOptions::default())?;
        // println!("{program:?}");
        let mut interpreter = Interpreter::new(program);

        serialize_into_sink(&mut interpreter, items)?;

        interpreter.build_arrow_arrays()
    }
}

mod primitives {
    use super::*;

    #[derive(Debug, Serialize, Deserialize)]
    pub struct Item {
        pub a: u8,
        pub b: u16,
        pub c: u32,
        pub d: u64,
        pub e: i8,
        pub f: i16,
        pub g: i32,
        pub h: i64,
        pub i: f32,
        pub j: f64,
        pub k: bool,
    }

    impl Item {
        pub fn random<R: Rng + ?Sized>(rng: &mut R) -> Self {
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
}

fn benchmark_serialize_arrow2_primitives(c: &mut Criterion) {
    let mut group = c.benchmark_group("serialize_arrow2_primitives");
    group.sample_size(20);
    group.sampling_mode(criterion::SamplingMode::Flat);
    group.measurement_time(Duration::from_secs(60));

    let mut rng = rand::thread_rng();

    let items = (0..100_000)
        .map(|_| primitives::Item::random(&mut rng))
        .collect::<Vec<_>>();
    let fields = arrow2::serialize_into_fields(&items, Default::default()).unwrap();

    group.bench_function("serde_arrow_bytecode", |b| {
        b.iter(|| black_box(bytecode::serialize(&fields, &items).unwrap()));
    });

    group.bench_function("serde_arrow", |b| {
        b.iter(|| black_box(arrow2::serialize_into_arrays(&fields, &items).unwrap()));
    });

    group.bench_function("manual", |b| {
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

    group.finish();
}

fn benchmark_deserialize_arrow2_primitives(c: &mut Criterion) {
    let mut group = c.benchmark_group("deserialize_arrow2_primitives");
    group.sample_size(20);
    group.sampling_mode(criterion::SamplingMode::Flat);
    group.measurement_time(Duration::from_secs(60));

    let mut rng = rand::thread_rng();

    let items = (0..100_000)
        .map(|_| primitives::Item::random(&mut rng))
        .collect::<Vec<_>>();
    let fields = arrow2::serialize_into_fields(&items, Default::default()).unwrap();
    let arrays = arrow2::serialize_into_arrays(&fields, &items).unwrap();

    group.bench_function("serde_arrow", |b| {
        b.iter(|| {
            black_box::<Vec<primitives::Item>>(
                arrow2::deserialize_from_arrays(&fields, &arrays).unwrap(),
            )
        });
    });

    group.bench_function("manual", |b| {
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
                res.push(primitives::Item {
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

fn benchmark_serialize_arrow2_complex(c: &mut Criterion) {
    let mut group = c.benchmark_group("serialize_arrow2_complex");
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
    let fields = arrow2::serialize_into_fields(&items, Default::default()).unwrap();

    group.bench_function("serde_arrow", |b| {
        b.iter(|| black_box(arrow2::serialize_into_arrays(&fields, &items).unwrap()));
    });

    group.bench_function("manual", |b| {
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
                        UnionMode::Dense,
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

    group.finish();
}

/// a simplified benchmark that is supported by arrow
fn benchmark_serialize_arrow_complex(c: &mut Criterion) {
    let mut group = c.benchmark_group("serialize_arrow_complex");
    group.sample_size(20);
    group.sampling_mode(criterion::SamplingMode::Flat);
    group.measurement_time(Duration::from_secs(60));

    #[derive(Debug, Serialize, Deserialize)]
    struct Item {
        string: String,
        points: Vec<Point>,
        child: SubItem,
    }

    #[derive(Debug, Serialize, Deserialize)]
    struct Point {
        x: f32,
        y: f32,
    }

    #[derive(Debug, Serialize, Deserialize)]
    struct SubItem {
        a: bool,
        b: f64,
        c: Option<f32>,
    }

    impl Item {
        fn random<R: Rng + ?Sized>(rng: &mut R) -> Self {
            let n_string = Uniform::new(1, 50).sample(rng);
            let n_points = Uniform::new(1, 50).sample(rng);

            Self {
                string: (0..n_string)
                    .map(|_| -> char { Standard.sample(rng) })
                    .collect(),
                points: (0..n_points)
                    .map(|_| Point {
                        x: Standard.sample(rng),
                        y: Standard.sample(rng),
                    })
                    .collect(),
                child: SubItem {
                    a: Standard.sample(rng),
                    b: Standard.sample(rng),
                    c: Standard.sample(rng),
                },
            }
        }
    }

    let mut rng = rand::thread_rng();

    let items = (0..100_000)
        .map(|_| Item::random(&mut rng))
        .collect::<Vec<_>>();
    let fields = arrow::serialize_into_fields(&items, Default::default()).unwrap();

    group.bench_function("serde_arrow_bytecode", |b| {
        b.iter(|| black_box(bytecode::serialize(&fields, &items).unwrap()));
    });

    group.bench_function("arrow", |b| {
        b.iter(|| {
            let schema = Schema::new(fields.clone());
            let mut decoder = ReaderBuilder::new(Arc::new(schema))
                .build_decoder()
                .unwrap();
            decoder.serialize(&items).unwrap();
            black_box(decoder.flush().unwrap().unwrap());
        });
    });

    group.bench_function("serde_arrow", |b| {
        b.iter(|| black_box(arrow::serialize_into_arrays(&fields, &items).unwrap()));
    });

    group.finish();
}

criterion_group!(
    benches,
    benchmark_serialize_arrow2_primitives,
    benchmark_deserialize_arrow2_primitives,
    benchmark_serialize_arrow2_complex,
    benchmark_serialize_arrow_complex,
);
criterion_main!(benches);
