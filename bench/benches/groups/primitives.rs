use std::ops::Range;

use arrow2_convert::{ArrowDeserialize, ArrowField, ArrowSerialize};
use rand::{
    Rng,
    distributions::{Standard, Uniform},
    prelude::Distribution,
};
use serde::{Deserialize, Serialize};

// required for arrow2_convert
use serde_arrow::_impl::arrow2;

#[derive(Debug, Serialize, Deserialize, ArrowField, ArrowSerialize, ArrowDeserialize)]
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

    use crate::impls::serde_arrow_arrow;
    super::bench_impl!(group, serde_arrow_arrow, items);

    use crate::impls::serde_arrow_marrow;
    super::bench_impl!(group, serde_arrow_marrow, items);

    use crate::impls::arrow;
    super::bench_impl!(group, arrow, items);

    use crate::impls::arrow2_convert;
    super::bench_impl!(group, arrow2_convert, items);

    group.finish();
}

criterion::criterion_group!(benchmark, benchmark_serialize);
