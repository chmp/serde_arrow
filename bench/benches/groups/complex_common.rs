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
    string: String,
    points: Vec<Point>,
    child: SubItem,
}

#[derive(Debug, Serialize, Deserialize, ArrowField, ArrowSerialize, ArrowDeserialize)]
struct Point {
    x: f32,
    y: f32,
}

#[derive(Debug, Serialize, Deserialize, ArrowField, ArrowSerialize, ArrowDeserialize)]
struct SubItem {
    a: bool,
    b: f64,
    c: Option<f32>,
}

impl Item {
    pub fn random<R: Rng + ?Sized>(rng: &mut R) -> Self {
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

crate::groups::impls::define_benchmark!(complex_common, ty = Item, n = [100_000, 1_000_000],);
