use arrow2_convert::{ArrowDeserialize, ArrowField, ArrowSerialize};
use rand::{distributions::Standard, prelude::Distribution, Rng};
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
            l: crate::groups::impls::random_string(rng, 0..50),
        }
    }
}

crate::groups::impls::define_benchmark!(primitives, ty = Item, n = [100_000, 1_000_000],);
