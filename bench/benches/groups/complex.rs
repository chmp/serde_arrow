use std::sync::Arc;

use arrow_array::{
    builder::{
        BooleanBuilder, Float32Builder, Float64Builder, LargeListBuilder, LargeStringBuilder,
        StructBuilder,
    },
    ArrayRef,
};
use arrow_schema::{DataType, Field};
use rand::{
    distributions::{Standard, Uniform},
    prelude::Distribution,
    Rng,
};
use serde::{Deserialize, Serialize};

#[derive(Debug, Serialize, Deserialize)]
pub struct Item {
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

    pub fn trace(_items: &[Item]) {}

    pub fn serialize(_fields: &(), items: &[Item]) -> Vec<ArrayRef> {
        vec![string_array(items), points_array(items), child_array(items)]
    }

    fn string_array(items: &[Item]) -> ArrayRef {
        let data_len = items.iter().map(|item| item.string.len()).sum();
        let mut builder = LargeStringBuilder::with_capacity(items.len(), data_len);
        for item in items {
            builder.append_value(&item.string);
        }
        Arc::new(builder.finish())
    }

    fn points_array(items: &[Item]) -> ArrayRef {
        let total_points = items.iter().map(|item| item.points.len()).sum();
        let point_fields = vec![
            Field::new("x", DataType::Float32, false),
            Field::new("y", DataType::Float32, false),
        ];
        let point_builders = vec![
            Box::new(Float32Builder::with_capacity(total_points))
                as Box<dyn arrow_array::builder::ArrayBuilder>,
            Box::new(Float32Builder::with_capacity(total_points)),
        ];
        let point_builder = StructBuilder::new(point_fields, point_builders);
        let mut builder = LargeListBuilder::with_capacity(point_builder, items.len());

        for item in items {
            for point in &item.points {
                builder
                    .values()
                    .field_builder::<Float32Builder>(0)
                    .unwrap()
                    .append_value(point.x);
                builder
                    .values()
                    .field_builder::<Float32Builder>(1)
                    .unwrap()
                    .append_value(point.y);
                builder.values().append(true);
            }
            builder.append(true);
        }

        Arc::new(builder.finish())
    }

    fn child_array(items: &[Item]) -> ArrayRef {
        let child_fields = vec![
            Field::new("first", DataType::Boolean, false),
            Field::new("second", DataType::Float64, false),
            Field::new("c", DataType::Float32, true),
        ];
        let child_builders = vec![
            Box::new(BooleanBuilder::with_capacity(items.len()))
                as Box<dyn arrow_array::builder::ArrayBuilder>,
            Box::new(Float64Builder::with_capacity(items.len())),
            Box::new(Float32Builder::with_capacity(items.len())),
        ];
        let mut builder = StructBuilder::new(child_fields, child_builders);
        for item in items {
            builder
                .field_builder::<BooleanBuilder>(0)
                .unwrap()
                .append_value(item.child.first);
            builder
                .field_builder::<Float64Builder>(1)
                .unwrap()
                .append_value(item.child.second);
            builder
                .field_builder::<Float32Builder>(2)
                .unwrap()
                .append_option(item.child.c);
            builder.append(true);
        }

        Arc::new(builder.finish())
    }
}
