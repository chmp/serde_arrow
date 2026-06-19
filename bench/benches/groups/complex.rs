use rand::{
    distributions::{Standard, Uniform},
    prelude::Distribution,
    Rng,
};
use serde::{Deserialize, Serialize};
use serde_arrow::marrow::{
    array::{Array, BooleanArray, BytesArray, ListArray, PrimitiveArray, StructArray},
    bits,
    datatypes::FieldMeta,
};

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

    use self::marrow_arrays;
    super::bench_impl!(group, marrow_arrays, items);

    use crate::impls::serde_arrow_arrow;
    super::bench_impl!(group, serde_arrow_arrow, items);

    use crate::impls::serde_arrow_marrow;
    super::bench_impl!(group, serde_arrow_marrow, items);

    use crate::impls::arrow;
    super::bench_impl!(group, arrow, items);

    group.finish();
}

criterion::criterion_group!(benchmark, benchmark_serialize);

mod marrow_arrays {
    use super::*;

    pub fn trace(_items: &[Item]) {}

    pub fn serialize(_fields: &(), items: &[Item]) -> Vec<Array> {
        vec![
            Array::LargeUtf8(bytes_array(items, |item| item.string.as_bytes())),
            points_array(items),
            child_array(items),
        ]
    }

    fn points_array(items: &[Item]) -> Array {
        let mut offsets = Vec::with_capacity(items.len() + 1);
        let mut points = Vec::new();
        offsets.push(0);

        for item in items {
            points.extend(&item.points);
            offsets.push(points.len() as i64);
        }

        Array::LargeList(ListArray {
            validity: None,
            offsets,
            meta: field_meta("item", false),
            elements: Box::new(Array::Struct(StructArray {
                len: points.len(),
                validity: None,
                fields: vec![
                    (
                        field_meta("x", false),
                        primitive_array(&points, |point| point.x, Array::Float32),
                    ),
                    (
                        field_meta("y", false),
                        primitive_array(&points, |point| point.y, Array::Float32),
                    ),
                ],
            })),
        })
    }

    fn child_array(items: &[Item]) -> Array {
        Array::Struct(StructArray {
            len: items.len(),
            validity: None,
            fields: vec![
                (
                    field_meta("first", false),
                    Array::Boolean(BooleanArray {
                        len: items.len(),
                        validity: None,
                        values: bit_vec(items.iter().map(|item| item.child.first)),
                    }),
                ),
                (
                    field_meta("second", false),
                    primitive_array(items, |item| item.child.second, Array::Float64),
                ),
                (
                    field_meta("c", true),
                    optional_f32_array(items, |item| item.child.c),
                ),
            ],
        })
    }

    fn primitive_array<T: Copy, I>(
        items: &[I],
        value: impl Fn(&I) -> T,
        array: impl FnOnce(PrimitiveArray<T>) -> Array,
    ) -> Array {
        array(PrimitiveArray {
            validity: None,
            values: items.iter().map(value).collect(),
        })
    }

    fn optional_f32_array(items: &[Item], value: impl Fn(&Item) -> Option<f32>) -> Array {
        Array::Float32(PrimitiveArray {
            validity: Some(bit_vec(items.iter().map(|item| value(item).is_some()))),
            values: items
                .iter()
                .map(|item| value(item).unwrap_or_default())
                .collect(),
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

    fn field_meta(name: &str, nullable: bool) -> FieldMeta {
        FieldMeta {
            name: name.to_owned(),
            nullable,
            metadata: Default::default(),
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
