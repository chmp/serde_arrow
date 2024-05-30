use std::collections::HashMap;

use serde::{Deserialize, Serialize};

use super::macros::expect_error;
use crate::internal::{
    utils::{Item, Items},
    schema::{GenericDataType as T, GenericField as F, Strategy},
    tracing::{Tracer, TracingOptions},
};

fn trace_type<'de, T: Deserialize<'de>>(options: TracingOptions) -> F {
    let mut tracer = Tracer::new(String::from("$"), options);
    tracer.trace_type::<Item<T>>().unwrap();

    let schema = tracer.to_schema().unwrap();
    schema.fields.into_iter().next().unwrap()
}

#[test]
fn issue_90() {
    #[derive(Deserialize)]
    pub struct Distribution {
        pub samples: Vec<f64>,
        pub statistic: String,
    }

    #[derive(Deserialize)]
    pub struct VectorMetric {
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

mod mixed_tracing_dates {
    use super::*;

    #[derive(Serialize, Deserialize)]
    struct Example {
        opt: Option<u32>,
        date: String,
    }

    fn expected() -> Vec<F> {
        vec![
            F::new("opt", T::U32, true),
            F::new("date", T::Date64, false).with_strategy(Strategy::UtcStrAsDate64),
        ]
    }

    fn samples() -> Vec<Example> {
        vec![Example {
            opt: None,
            date: String::from("2015-09-18T23:56:04Z"),
        }]
    }

    #[test]
    fn type_then_samples() {
        let mut tracer = Tracer::new(
            String::from("$"),
            TracingOptions::default().guess_dates(true),
        );

        tracer.trace_type::<Example>().unwrap();
        tracer.trace_samples(&samples()).unwrap();

        let actual = tracer.to_schema().unwrap().fields;
        assert_eq!(actual, expected());
    }

    #[test]
    fn samples_then_type() {
        let mut tracer = Tracer::new(
            String::from("$"),
            TracingOptions::default().guess_dates(true),
        );

        tracer.trace_samples(&samples()).unwrap();
        tracer.trace_type::<Example>().unwrap();

        let actual = tracer.to_schema().unwrap().fields;
        assert_eq!(actual, expected());
    }

    #[test]
    fn invalid_values_first() {
        let mut tracer = Tracer::new(
            String::from("$"),
            TracingOptions::default().guess_dates(true),
        );

        tracer.trace_samples(&Items(["foo bar"])).unwrap();
        tracer.trace_type::<Item<String>>().unwrap();
        tracer
            .trace_samples(&Items(["2015-09-18T23:56:04Z"]))
            .unwrap();

        let actual = tracer
            .to_schema()
            .unwrap()
            .fields
            .into_iter()
            .next()
            .unwrap();
        let expected = F::new("item", T::LargeUtf8, false);

        assert_eq!(actual, expected);
    }

    #[test]
    fn invalid_values_last() {
        let mut tracer = Tracer::new(
            String::from("$"),
            TracingOptions::default().guess_dates(true),
        );

        tracer
            .trace_samples(&Items(["2015-09-18T23:56:04Z"]))
            .unwrap();
        tracer.trace_type::<Item<String>>().unwrap();
        tracer.trace_samples(&Items(["foo bar"])).unwrap();

        let actual = tracer
            .to_schema()
            .unwrap()
            .fields
            .into_iter()
            .next()
            .unwrap();
        let expected = F::new("item", T::LargeUtf8, false);

        assert_eq!(actual, expected);
    }
}

mod mixed_tracing_unions {
    use crate::internal::{
        utils::{Item, Items},
        tracing,
    };

    use super::*;

    #[test]
    fn example() {
        #[derive(Serialize, Deserialize)]
        enum E {
            A,
            B,
            C(u32),
        }

        let mut tracer = tracing::Tracer::new(
            String::from("$"),
            TracingOptions::default().allow_null_fields(true),
        );
        tracer.trace_type::<Item<E>>().unwrap();
        tracer.trace_samples(&Items(&[E::A, E::C(32)])).unwrap();
        let schema = tracer.to_schema().unwrap();

        let actual = schema.fields.into_iter().next().unwrap();
        let expected = F::new("item", T::Union, false)
            .with_child(F::new("A", T::Null, true))
            .with_child(F::new("B", T::Null, true))
            .with_child(F::new("C", T::U32, false));

        assert_eq!(actual, expected);
    }
}

#[test]
fn unsupported_recursive_types() {
    #[allow(unused)]
    #[derive(Deserialize)]
    struct Tree {
        left: Option<Box<Tree>>,
        right: Option<Box<Tree>>,
    }

    let mut tracer = Tracer::new(String::from("$"), TracingOptions::default());
    let res = tracer.trace_type::<Tree>();
    expect_error(&res, "too deeply nested type detected");
}
