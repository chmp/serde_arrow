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

#[derive(Debug, Serialize, Deserialize)]
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

    use crate::impls::arrow;
    super::bench_impl!(group, arrow, items);

    group.finish();
}

criterion::criterion_group!(benchmark, benchmark_serialize);

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
