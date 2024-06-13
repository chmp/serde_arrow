use std::collections::HashMap;

use serde::Deserialize;

use crate::internal::{
    schema::{tracer::Tracer, GenericDataType as T, GenericField as F, Strategy, TracingOptions},
    testing::assert_error,
    utils::Item,
};

fn trace_type<'de, T: Deserialize<'de>>(options: TracingOptions) -> F {
    let tracer = Tracer::from_type::<Item<T>>(options).unwrap();
    let schema = tracer.to_schema().unwrap();
    schema.fields.into_iter().next().unwrap()
}

#[test]
fn issue_90() {
    #[derive(Deserialize)]
    pub struct Distribution {
        #[allow(unused)]
        pub samples: Vec<f64>,
        #[allow(unused)]
        pub statistic: String,
    }

    #[derive(Deserialize)]
    pub struct VectorMetric {
        #[allow(unused)]
        pub distribution: Option<Distribution>,
    }

    let actual = trace_type::<VectorMetric>(TracingOptions::default());
    let expected = F::new("item", T::Struct, false).with_child(
        F::new("distribution", T::Struct, true)
            .with_child(F::new("samples", T::LargeList, false).with_child(F::new(
                "element",
                T::F64,
                false,
            )))
            .with_child(F::new("statistic", T::LargeUtf8, false)),
    );

    assert_eq!(actual, expected);
}

#[test]
fn trace_primitives() {
    assert_eq!(
        trace_type::<()>(TracingOptions::default().allow_null_fields(true)),
        F::new("item", T::Null, true),
    );
    assert_eq!(
        trace_type::<i8>(TracingOptions::default()),
        F::new("item", T::I8, false)
    );
    assert_eq!(
        trace_type::<i16>(TracingOptions::default()),
        F::new("item", T::I16, false)
    );
    assert_eq!(
        trace_type::<i32>(TracingOptions::default()),
        F::new("item", T::I32, false)
    );
    assert_eq!(
        trace_type::<i64>(TracingOptions::default()),
        F::new("item", T::I64, false)
    );

    assert_eq!(
        trace_type::<u8>(TracingOptions::default()),
        F::new("item", T::U8, false)
    );
    assert_eq!(
        trace_type::<u16>(TracingOptions::default()),
        F::new("item", T::U16, false)
    );
    assert_eq!(
        trace_type::<u32>(TracingOptions::default()),
        F::new("item", T::U32, false)
    );
    assert_eq!(
        trace_type::<u64>(TracingOptions::default()),
        F::new("item", T::U64, false)
    );

    assert_eq!(
        trace_type::<f32>(TracingOptions::default()),
        F::new("item", T::F32, false)
    );
    assert_eq!(
        trace_type::<f64>(TracingOptions::default()),
        F::new("item", T::F64, false)
    );
}

#[test]
fn trace_option() {
    assert_eq!(
        trace_type::<i8>(TracingOptions::default()),
        F::new("item", T::I8, false)
    );
    assert_eq!(
        trace_type::<Option<i8>>(TracingOptions::default()),
        F::new("item", T::I8, true)
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
    let expected = F::new("item", T::Struct, false)
        .with_child(F::new("a", T::Bool, false))
        .with_child(F::new("b", T::I8, true));

    assert_eq!(actual, expected);
}

#[test]
fn trace_tuple_as_struct() {
    let actual = trace_type::<(bool, Option<i8>)>(TracingOptions::default());
    let expected = F::new("item", T::Struct, false)
        .with_child(F::new("0", T::Bool, false))
        .with_child(F::new("1", T::I8, true))
        .with_strategy(Strategy::TupleAsStruct);

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
    let expected = F::new("item", T::Union, false)
        .with_child(F::new("A", T::I8, false))
        .with_child(F::new("B", T::F32, false));

    assert_eq!(actual, expected);
}

#[test]
fn trace_list() {
    let actual = trace_type::<Vec<String>>(TracingOptions::default());
    let expected =
        F::new("item", T::LargeList, false).with_child(F::new("element", T::LargeUtf8, false));

    assert_eq!(actual, expected);
}

#[test]
fn trace_map() {
    let actual = trace_type::<HashMap<i8, String>>(TracingOptions::default().map_as_struct(false));
    let expected = F::new("item", T::Map, false).with_child(
        F::new("entries", T::Struct, false)
            .with_child(F::new("key", T::I8, false))
            .with_child(F::new("value", T::LargeUtf8, false)),
    );

    assert_eq!(actual, expected);
}

#[test]
fn unsupported_recursive_types() {
    #[allow(unused)]
    #[derive(Deserialize)]
    struct Tree {
        left: Option<Box<Tree>>,
        right: Option<Box<Tree>>,
    }

    let res = Tracer::from_type::<Tree>(TracingOptions::default());
    assert_error(&res, "too deeply nested type detected");
}
