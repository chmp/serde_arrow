use std::collections::HashMap;

use marrow::datatypes::{DataType, Field, UnionMode};
use serde::Deserialize;
use serde_json::json;

use crate::internal::{
    schema::{tracer::Tracer, transmute_field, Strategy, TracingOptions, STRATEGY_KEY},
    utils::Item,
};

fn trace_type<'de, T: Deserialize<'de>>(options: TracingOptions) -> Field {
    let tracer = Tracer::from_type::<Item<T>>(options).unwrap();
    let schema = tracer.to_schema().unwrap();
    schema.fields.into_iter().next().unwrap()
}

fn new_field(name: &str, nullable: bool, data_type: DataType) -> Field {
    Field {
        name: name.to_owned(),
        data_type,
        nullable,
        metadata: Default::default(),
    }
}

#[test]
fn issue_90() {
    #[allow(unused)]
    #[derive(Deserialize)]
    pub struct Distribution {
        pub samples: Vec<f64>,
        pub statistic: String,
    }

    #[allow(unused)]
    #[derive(Deserialize)]
    pub struct VectorMetric {
        pub distribution: Option<Distribution>,
    }

    let actual = trace_type::<VectorMetric>(TracingOptions::default());
    let expected = transmute_field(json!({
        "name": "item",
        "data_type": "Struct",
        "children": [
            {
                "name": "distribution",
                "nullable": true,
                "data_type": "Struct",
                "children": [
                    {
                        "name": "samples",
                        "data_type": "LargeList",
                        "children": [{"name": "element", "data_type": "F64"}],
                    },
                    {"name": "statistic", "data_type": "LargeUtf8"},
                ],
            },
        ],
    }))
    .unwrap();
    assert_eq!(actual, expected);
}

#[test]
fn trace_primitives() {
    assert_eq!(
        trace_type::<()>(TracingOptions::default().allow_null_fields(true)),
        new_field("item", true, DataType::Null),
    );
    assert_eq!(
        trace_type::<i8>(TracingOptions::default()),
        new_field("item", false, DataType::Int8)
    );
    assert_eq!(
        trace_type::<i16>(TracingOptions::default()),
        new_field("item", false, DataType::Int16)
    );
    assert_eq!(
        trace_type::<i32>(TracingOptions::default()),
        new_field("item", false, DataType::Int32)
    );
    assert_eq!(
        trace_type::<i64>(TracingOptions::default()),
        new_field("item", false, DataType::Int64)
    );

    assert_eq!(
        trace_type::<u8>(TracingOptions::default()),
        new_field("item", false, DataType::UInt8)
    );
    assert_eq!(
        trace_type::<u16>(TracingOptions::default()),
        new_field("item", false, DataType::UInt16)
    );
    assert_eq!(
        trace_type::<u32>(TracingOptions::default()),
        new_field("item", false, DataType::UInt32)
    );
    assert_eq!(
        trace_type::<u64>(TracingOptions::default()),
        new_field("item", false, DataType::UInt64)
    );

    assert_eq!(
        trace_type::<f32>(TracingOptions::default()),
        new_field("item", false, DataType::Float32)
    );
    assert_eq!(
        trace_type::<f64>(TracingOptions::default()),
        new_field("item", false, DataType::Float64)
    );
}

#[test]
fn trace_option() {
    assert_eq!(
        trace_type::<i8>(TracingOptions::default()),
        new_field("item", false, DataType::Int8)
    );
    assert_eq!(
        trace_type::<Option<i8>>(TracingOptions::default()),
        new_field("item", true, DataType::Int8)
    );
}

#[test]
fn trace_struct() {
    #[allow(dead_code)]
    #[derive(Deserialize)]
    struct Example {
        a: bool,
        b: Option<i8>,
    }

    let actual = trace_type::<Example>(TracingOptions::default());
    let expected = new_field(
        "item",
        false,
        DataType::Struct(vec![
            new_field("a", false, DataType::Boolean),
            new_field("b", true, DataType::Int8),
        ]),
    );

    assert_eq!(actual, expected);
}

#[test]
fn trace_tuple_as_struct() {
    let actual = trace_type::<(bool, Option<i8>)>(TracingOptions::default());

    let mut expected = new_field(
        "item",
        false,
        DataType::Struct(vec![
            new_field("0", false, DataType::Boolean),
            new_field("1", true, DataType::Int8),
        ]),
    );
    expected.metadata.insert(
        STRATEGY_KEY.to_string(),
        Strategy::TupleAsStruct.to_string(),
    );

    assert_eq!(actual, expected);
}

#[test]
fn trace_union() {
    #[allow(dead_code)]
    #[derive(Deserialize)]
    enum Example {
        A(i8),
        B(f32),
    }

    let actual = trace_type::<Example>(TracingOptions::default());
    let expected = new_field(
        "item",
        false,
        DataType::Union(
            vec![
                (0, new_field("A", false, DataType::Int8)),
                (1, new_field("B", false, DataType::Float32)),
            ],
            UnionMode::Dense,
        ),
    );

    assert_eq!(actual, expected);
}

#[test]
fn trace_list() {
    let actual = trace_type::<Vec<String>>(TracingOptions::default());
    let expected = new_field(
        "item",
        false,
        DataType::LargeList(Box::new(new_field("element", false, DataType::LargeUtf8))),
    );

    assert_eq!(actual, expected);
}

#[test]
fn trace_map() {
    let actual = trace_type::<HashMap<i8, String>>(TracingOptions::default().map_as_struct(false));
    let expected = new_field(
        "item",
        false,
        DataType::Map(
            Box::new(new_field(
                "entries",
                false,
                DataType::Struct(vec![
                    new_field("key", false, DataType::Int8),
                    new_field("value", false, DataType::LargeUtf8),
                ]),
            )),
            false,
        ),
    );
    assert_eq!(actual, expected);
}
