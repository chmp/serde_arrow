use rand::{
    Rng, SeedableRng,
    distributions::{Standard, Uniform},
    prelude::Distribution,
    rngs::StdRng,
};
use serde::{Deserialize, Serialize};
use serde_arrow::marrow::{
    array::{Array, BooleanArray, BytesArray, ListArray, PrimitiveArray, StructArray},
    datatypes::FieldMeta,
};

#[derive(Debug, Serialize, Deserialize)]
pub struct Item {
    pub(crate) string: String,
    pub(crate) points: Vec<Point>,
    pub(crate) child: SubItem,
}

#[derive(Debug, Serialize, Deserialize)]
pub(crate) struct Point {
    pub(crate) x: f32,
    pub(crate) y: f32,
}

#[derive(Debug, Serialize, Deserialize)]
pub(crate) struct SubItem {
    pub(crate) first: bool,
    pub(crate) second: f64,
    pub(crate) c: Option<f32>,
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

fn push_bit(values: &mut Vec<u8>, idx: usize, bit: bool) {
    let byte_idx = idx / 8;
    if values.len() <= byte_idx {
        values.resize(byte_idx + 1, 0);
    }
    if bit {
        values[byte_idx] |= 1 << (idx % 8);
    }
}

fn field_meta(name: &str, nullable: bool) -> FieldMeta {
    FieldMeta {
        name: name.into(),
        nullable,
        metadata: Default::default(),
    }
}

impl crate::impls::marrow_direct::DirectMarrowBuild for Item {
    fn build_marrow_arrays(items: &[Self]) -> Vec<Array> {
        let len = items.len();

        let mut string_offsets = Vec::with_capacity(len + 1);
        let mut string_data = Vec::new();

        let mut points_offsets = Vec::with_capacity(len + 1);
        let mut point_x = Vec::new();
        let mut point_y = Vec::new();

        let mut child_first_values = Vec::with_capacity(len.div_ceil(8));
        let mut child_second_values = Vec::with_capacity(len);
        let mut child_c_validity = Vec::with_capacity(len.div_ceil(8));
        let mut child_c_values = Vec::with_capacity(len);

        string_offsets.push(0);
        points_offsets.push(0);

        for (row_idx, item) in items.iter().enumerate() {
            let string_bytes = item.string.as_bytes();
            string_data.extend_from_slice(string_bytes);
            string_offsets
                .push(i32::try_from(string_data.len()).expect("string data offset overflow"));

            for point in &item.points {
                point_x.push(point.x);
                point_y.push(point.y);
            }
            points_offsets
                .push(i32::try_from(point_x.len()).expect("list element offset overflow"));

            push_bit(&mut child_first_values, row_idx, item.child.first);
            child_second_values.push(item.child.second);

            if let Some(v) = item.child.c {
                push_bit(&mut child_c_validity, row_idx, true);
                child_c_values.push(v);
            } else {
                push_bit(&mut child_c_validity, row_idx, false);
                child_c_values.push(f32::default());
            }
        }

        let points_elements = Array::Struct(StructArray {
            len: point_x.len(),
            validity: None,
            fields: vec![
                (
                    field_meta("x", false),
                    Array::Float32(PrimitiveArray {
                        validity: None,
                        values: point_x,
                    }),
                ),
                (
                    field_meta("y", false),
                    Array::Float32(PrimitiveArray {
                        validity: None,
                        values: point_y,
                    }),
                ),
            ],
        });

        let points_array = Array::List(ListArray {
            validity: None,
            offsets: points_offsets,
            elements: Box::new(points_elements),
            meta: field_meta("item", false),
        });

        let child_array = Array::Struct(StructArray {
            len,
            validity: None,
            fields: vec![
                (
                    field_meta("first", false),
                    Array::Boolean(BooleanArray {
                        len,
                        validity: None,
                        values: child_first_values,
                    }),
                ),
                (
                    field_meta("second", false),
                    Array::Float64(PrimitiveArray {
                        validity: None,
                        values: child_second_values,
                    }),
                ),
                (
                    field_meta("c", true),
                    Array::Float32(PrimitiveArray {
                        validity: Some(child_c_validity),
                        values: child_c_values,
                    }),
                ),
            ],
        });

        vec![
            Array::Utf8(BytesArray {
                validity: None,
                offsets: string_offsets,
                data: string_data,
            }),
            points_array,
            child_array,
        ]
    }
}

pub fn benchmark_serialize(c: &mut criterion::Criterion) {
    let mut group = super::new_group(c, "complex_1000");
    let mut rng = StdRng::seed_from_u64(0xFACE_FEED);

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
