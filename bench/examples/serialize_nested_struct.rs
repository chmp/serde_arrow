use rand::{
    Rng,
    distributions::{Standard, Uniform},
    prelude::Distribution,
};
use serde::Serialize;
use serde_arrow::marrow::datatypes::Field;
use serde_arrow::schema::SchemaLike;

const NUM_REPETITIONS: usize = 1_000;

fn main() {
    let items = (0..100)
        .map(|_| Item::random(&mut rand::thread_rng()))
        .collect::<Vec<_>>();

    let fields = Vec::<Field>::from_samples(&items, Default::default()).unwrap();

    for _ in 0..NUM_REPETITIONS {
        let arrays = serde_arrow::to_marrow(&fields, &items).unwrap();
        criterion::black_box(arrays);
    }
}

#[derive(Debug, Serialize)]
pub struct Item {
    string: String,
    points: Vec<Point>,
    child: SubItem,
}

#[derive(Debug, Serialize)]
struct Point {
    x: f32,
    y: f32,
}

#[derive(Debug, Serialize)]
struct SubItem {
    a: bool,
    b: f64,
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
            },
        }
    }
}
