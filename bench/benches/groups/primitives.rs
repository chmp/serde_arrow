use std::ops::Range;

use rand::{
    Rng, SeedableRng,
    distributions::{Standard, Uniform},
    prelude::Distribution,
    rngs::StdRng,
};
use serde::{Deserialize, Serialize};
use serde_arrow::marrow::array::{Array, BooleanArray, BytesArray, PrimitiveArray};

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

fn push_bit(values: &mut Vec<u8>, idx: usize, bit: bool) {
    let byte_idx = idx / 8;
    if values.len() <= byte_idx {
        values.resize(byte_idx + 1, 0);
    }
    if bit {
        values[byte_idx] |= 1 << (idx % 8);
    }
}

impl crate::impls::marrow_direct::DirectMarrowBuild for Item {
    fn build_marrow_arrays(items: &[Self]) -> Vec<Array> {
        let len = items.len();

        let mut k_values = Vec::with_capacity(len.div_ceil(8));
        let mut a_values = Vec::with_capacity(len);
        let mut b_values = Vec::with_capacity(len);
        let mut c_values = Vec::with_capacity(len);
        let mut d_values = Vec::with_capacity(len);
        let mut e_values = Vec::with_capacity(len);
        let mut f_values = Vec::with_capacity(len);
        let mut g_values = Vec::with_capacity(len);
        let mut h_values = Vec::with_capacity(len);
        let mut i_values = Vec::with_capacity(len);
        let mut j_values = Vec::with_capacity(len);
        let mut l_offsets = Vec::with_capacity(len + 1);
        let mut l_data = Vec::new();

        l_offsets.push(0);

        for (idx, item) in items.iter().enumerate() {
            push_bit(&mut k_values, idx, item.k);
            a_values.push(item.a);
            b_values.push(item.b);
            c_values.push(item.c);
            d_values.push(item.d);
            e_values.push(item.e);
            f_values.push(item.f);
            g_values.push(item.g);
            h_values.push(item.h);
            i_values.push(item.i);
            j_values.push(item.j);

            let bytes = item.l.as_bytes();
            l_data.extend_from_slice(bytes);
            l_offsets.push(i32::try_from(l_data.len()).expect("string data offset overflow"));
        }

        vec![
            Array::Boolean(BooleanArray {
                len,
                validity: None,
                values: k_values,
            }),
            Array::UInt8(PrimitiveArray {
                validity: None,
                values: a_values,
            }),
            Array::UInt16(PrimitiveArray {
                validity: None,
                values: b_values,
            }),
            Array::UInt32(PrimitiveArray {
                validity: None,
                values: c_values,
            }),
            Array::UInt64(PrimitiveArray {
                validity: None,
                values: d_values,
            }),
            Array::Int8(PrimitiveArray {
                validity: None,
                values: e_values,
            }),
            Array::Int16(PrimitiveArray {
                validity: None,
                values: f_values,
            }),
            Array::Int32(PrimitiveArray {
                validity: None,
                values: g_values,
            }),
            Array::Int64(PrimitiveArray {
                validity: None,
                values: h_values,
            }),
            Array::Float32(PrimitiveArray {
                validity: None,
                values: i_values,
            }),
            Array::Float64(PrimitiveArray {
                validity: None,
                values: j_values,
            }),
            Array::Utf8(BytesArray {
                validity: None,
                offsets: l_offsets,
                data: l_data,
            }),
        ]
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
    let mut rng = StdRng::seed_from_u64(0xCAFE_BABE);

    let items = (0..1_000)
        .map(|_| Item::random(&mut rng))
        .collect::<Vec<_>>();

    use crate::impls::serde_arrow_arrow;
    super::bench_impl!(group, serde_arrow_arrow, items);

    use crate::impls::serde_arrow_marrow;
    super::bench_impl!(group, serde_arrow_marrow, items);

    use crate::impls::arrow;
    super::bench_impl!(group, arrow, items);

    use crate::impls::marrow_direct;
    super::bench_impl!(group, marrow_direct, items);

    group.finish();
}

criterion::criterion_group!(benchmark, benchmark_serialize);
