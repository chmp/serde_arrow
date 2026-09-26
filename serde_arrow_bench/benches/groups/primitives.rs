use std::{ops::Range, sync::Arc};

use arrow_array::{
    builder::{
        BooleanBuilder, Float32Builder, Float64Builder, Int16Builder, Int32Builder, Int64Builder,
        Int8Builder, LargeStringBuilder, UInt16Builder, UInt32Builder, UInt64Builder, UInt8Builder,
    },
    ArrayRef,
};
use rand::{
    distributions::{Standard, Uniform},
    prelude::Distribution,
    Rng,
};
use serde::{Deserialize, Serialize};

#[derive(Debug, PartialEq, Serialize, Deserialize)]
pub struct Item {
    pub k: bool,
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
    pub l: String,
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
            l: random_string(rng, 0..50),
        }
    }
}

pub fn random_string<R: Rng + ?Sized>(rng: &mut R, length: Range<usize>) -> String {
    let n_string = Uniform::new(length.start, length.end).sample(rng);

    (0..n_string)
        .map(|_| -> char { Standard.sample(rng) })
        .collect()
}

pub fn benchmark_serialize(c: &mut criterion::Criterion) {
    let mut group = super::new_group(c, "primitives_1000");

    let items = (0..1_000)
        .map(|_| Item::random(&mut rand::thread_rng()))
        .collect::<Vec<_>>();

    use self::arrow_builder;
    super::bench_impl!(group, arrow_builder, items);

    use crate::impls::serde_arrow_arrow;
    super::bench_impl!(group, serde_arrow_arrow, items);

    use crate::impls::serde_arrow_marrow;
    super::bench_impl!(group, serde_arrow_marrow, items);

    let fields_marrow = serde_arrow_marrow::trace(&items);
    group.bench_function("serde_arrow_marrow_push", |b| {
        b.iter(|| {
            criterion::black_box(serde_arrow_marrow::serialize_by_push(
                &fields_marrow,
                &items,
            ))
        })
    });

    use crate::impls::serde_arrow_marrow_to_arrow;
    super::bench_impl!(group, serde_arrow_marrow_to_arrow, items);

    use crate::impls::arrow;
    super::bench_impl!(group, arrow, items);

    group.finish();
}

pub fn benchmark_deserialize(c: &mut criterion::Criterion) {
    let items = (0..1_000)
        .map(|_| Item::random(&mut rand::thread_rng()))
        .collect::<Vec<_>>();
    let mut group = super::new_group(c, "primitives_1000_deserialize");

    let arrow_fields = crate::impls::serde_arrow_arrow::trace(&items);
    let arrow_arrays = serde_arrow::to_arrow(&arrow_fields, &items).unwrap();
    assert_eq!(arrow_manual::deserialize(&arrow_arrays), items);
    group.bench_function("arrow_manual", |b| {
        b.iter(|| criterion::black_box(arrow_manual::deserialize(&arrow_arrays)))
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
    group.bench_function("serde_arrow_marrow_iter", |b| {
        b.iter(|| {
            let deserializer =
                serde_arrow::Deserializer::from_marrow(&marrow_fields, &marrow_views).unwrap();
            let decoded = deserializer
                .iter()
                .map(Item::deserialize)
                .collect::<serde_arrow::Result<Vec<_>>>()
                .unwrap();
            criterion::black_box(decoded)
        })
    });

    group.finish();
}

criterion::criterion_group!(benchmark, benchmark_serialize, benchmark_deserialize);

mod arrow_manual {
    use super::*;
    use arrow_array::{
        BooleanArray, Float32Array, Float64Array, Int16Array, Int32Array, Int64Array, Int8Array,
        LargeStringArray, UInt16Array, UInt32Array, UInt64Array, UInt8Array,
    };

    pub fn deserialize(arrays: &[ArrayRef]) -> Vec<Item> {
        let k = arrays[0].as_any().downcast_ref::<BooleanArray>().unwrap();
        let a = arrays[1].as_any().downcast_ref::<UInt8Array>().unwrap();
        let b = arrays[2].as_any().downcast_ref::<UInt16Array>().unwrap();
        let c = arrays[3].as_any().downcast_ref::<UInt32Array>().unwrap();
        let d = arrays[4].as_any().downcast_ref::<UInt64Array>().unwrap();
        let e = arrays[5].as_any().downcast_ref::<Int8Array>().unwrap();
        let f = arrays[6].as_any().downcast_ref::<Int16Array>().unwrap();
        let g = arrays[7].as_any().downcast_ref::<Int32Array>().unwrap();
        let h = arrays[8].as_any().downcast_ref::<Int64Array>().unwrap();
        let i = arrays[9].as_any().downcast_ref::<Float32Array>().unwrap();
        let j = arrays[10].as_any().downcast_ref::<Float64Array>().unwrap();
        let l = arrays[11]
            .as_any()
            .downcast_ref::<LargeStringArray>()
            .unwrap();

        (0..k.len())
            .map(|row| Item {
                k: k.value(row),
                a: a.value(row),
                b: b.value(row),
                c: c.value(row),
                d: d.value(row),
                e: e.value(row),
                f: f.value(row),
                g: g.value(row),
                h: h.value(row),
                i: i.value(row),
                j: j.value(row),
                l: l.value(row).to_owned(),
            })
            .collect()
    }
}

mod arrow_builder {
    use super::*;

    macro_rules! primitive_array {
        ($items:expr, $builder:ty, $field:ident) => {{
            let mut builder = <$builder>::with_capacity($items.len());
            for item in $items {
                builder.append_value(item.$field);
            }
            Arc::new(builder.finish()) as ArrayRef
        }};
    }

    pub fn trace(_items: &[Item]) {}

    pub fn serialize(_fields: &(), items: &[Item]) -> Vec<ArrayRef> {
        vec![
            boolean_array(items),
            primitive_array!(items, UInt8Builder, a),
            primitive_array!(items, UInt16Builder, b),
            primitive_array!(items, UInt32Builder, c),
            primitive_array!(items, UInt64Builder, d),
            primitive_array!(items, Int8Builder, e),
            primitive_array!(items, Int16Builder, f),
            primitive_array!(items, Int32Builder, g),
            primitive_array!(items, Int64Builder, h),
            primitive_array!(items, Float32Builder, i),
            primitive_array!(items, Float64Builder, j),
            string_array(items),
        ]
    }

    fn boolean_array(items: &[Item]) -> ArrayRef {
        let mut builder = BooleanBuilder::with_capacity(items.len());
        for item in items {
            builder.append_value(item.k);
        }
        Arc::new(builder.finish())
    }

    fn string_array(items: &[Item]) -> ArrayRef {
        let data_len = items.iter().map(|item| item.l.len()).sum();
        let mut builder = LargeStringBuilder::with_capacity(items.len(), data_len);
        for item in items {
            builder.append_value(&item.l);
        }
        Arc::new(builder.finish())
    }
}
