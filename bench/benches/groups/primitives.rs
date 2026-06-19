use std::ops::Range;

use rand::{
    distributions::{Standard, Uniform},
    prelude::Distribution,
    Rng,
};
use serde::{Deserialize, Serialize};
use serde_arrow::marrow::{
    array::{Array, BooleanArray, BytesArray, PrimitiveArray},
    bits,
};

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

    use self::marrow_to_arrow;
    super::bench_impl!(group, marrow_to_arrow, items);

    use self::marrow;
    super::bench_impl!(group, marrow, items);

    use crate::impls::serde_arrow_arrow;
    super::bench_impl!(group, serde_arrow_arrow, items);

    use crate::impls::serde_arrow_marrow;
    super::bench_impl!(group, serde_arrow_marrow, items);

    use crate::impls::arrow;
    super::bench_impl!(group, arrow, items);

    group.finish();
}

criterion::criterion_group!(benchmark, benchmark_serialize);

mod marrow_to_arrow {
    use super::*;

    pub fn trace(_items: &[Item]) {}

    pub fn serialize(
        _fields: &(),
        items: &[Item],
    ) -> Vec<serde_arrow::_impl::arrow::array::ArrayRef> {
        crate::impls::marrow_to_arrow_arrays(super::marrow::serialize(&(), items))
    }
}

mod marrow {
    use super::*;

    pub fn trace(_items: &[Item]) {}

    pub fn serialize(_fields: &(), items: &[Item]) -> Vec<Array> {
        vec![
            Array::Boolean(BooleanArray {
                len: items.len(),
                validity: None,
                values: bit_vec(items.iter().map(|item| item.k)),
            }),
            primitive_array(items, |item| item.a, Array::UInt8),
            primitive_array(items, |item| item.b, Array::UInt16),
            primitive_array(items, |item| item.c, Array::UInt32),
            primitive_array(items, |item| item.d, Array::UInt64),
            primitive_array(items, |item| item.e, Array::Int8),
            primitive_array(items, |item| item.f, Array::Int16),
            primitive_array(items, |item| item.g, Array::Int32),
            primitive_array(items, |item| item.h, Array::Int64),
            primitive_array(items, |item| item.i, Array::Float32),
            primitive_array(items, |item| item.j, Array::Float64),
            Array::LargeUtf8(bytes_array(items, |item| item.l.as_bytes())),
        ]
    }

    fn primitive_array<T: Copy>(
        items: &[Item],
        value: impl Fn(&Item) -> T,
        array: impl FnOnce(PrimitiveArray<T>) -> Array,
    ) -> Array {
        array(PrimitiveArray {
            validity: None,
            values: items.iter().map(value).collect(),
        })
    }

    fn bytes_array<'a>(items: &'a [Item], value: impl Fn(&'a Item) -> &'a [u8]) -> BytesArray<i64> {
        let mut offsets = Vec::with_capacity(items.len() + 1);
        let mut data = Vec::new();
        offsets.push(0);

        for item in items {
            data.extend_from_slice(value(item));
            offsets.push(data.len() as i64);
        }

        BytesArray {
            validity: None,
            offsets,
            data,
        }
    }

    fn bit_vec(values: impl IntoIterator<Item = bool>) -> Vec<u8> {
        let mut res = Vec::new();
        let mut len = 0;
        for value in values {
            bits::push(&mut res, &mut len, value);
        }
        res
    }
}
