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
    first: bool,
    second: f64,
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
                first: Standard.sample(rng),
                second: Standard.sample(rng),
                c: Standard.sample(rng),
            },
        }
    }
}

pub fn benchmark_serialize(c: &mut criterion::Criterion) {
    let mut group = super::new_group(c, "complex_1000");

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
