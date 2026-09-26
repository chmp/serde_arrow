use std::sync::Arc;

use arrow_array::{ArrayRef, Int64Array};
use arrow_schema::{DataType as ArrowDataType, Field as ArrowField, FieldRef};
use criterion::Criterion;
use serde_arrow::{
    marrow::datatypes::{DataType, Field},
    ArrayBuilder, Deserializer,
};

const NUM_FIELDS: usize = 1_024;

pub fn benchmark_setup(c: &mut Criterion) {
    let mut group = super::new_group(c, "wide_schema_1024");
    let marrow_fields = nested_marrow_fields();
    group.bench_function("builder_setup", |b| {
        b.iter(|| criterion::black_box(ArrayBuilder::from_marrow(&marrow_fields).unwrap()))
    });

    let arrow_fields = arrow_fields();
    let arrays = (0..NUM_FIELDS)
        .map(|_| Arc::new(Int64Array::from(Vec::<i64>::new())) as ArrayRef)
        .collect::<Vec<_>>();
    group.bench_function("deserializer_setup", |b| {
        b.iter(|| criterion::black_box(Deserializer::from_arrow(&arrow_fields, &arrays).unwrap()))
    });

    group.finish();
}

fn nested_marrow_fields() -> Vec<Field> {
    vec![Field {
        name: "record".into(),
        data_type: DataType::Struct(
            (0..NUM_FIELDS)
                .map(|idx| Field {
                    name: format!("field_{idx}"),
                    data_type: DataType::Int64,
                    nullable: false,
                    metadata: Default::default(),
                })
                .collect(),
        ),
        nullable: false,
        metadata: Default::default(),
    }]
}

fn arrow_fields() -> Vec<FieldRef> {
    (0..NUM_FIELDS)
        .map(|idx| {
            Arc::new(ArrowField::new(
                format!("field_{idx}"),
                ArrowDataType::Int64,
                false,
            ))
        })
        .collect()
}

criterion::criterion_group!(benchmark, benchmark_setup);
