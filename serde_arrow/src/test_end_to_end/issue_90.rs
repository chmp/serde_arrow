use serde::{Deserialize, Serialize};

use crate::{
    arrow::serialize_into_arrays,
    schema::{Schema, TracingOptions},
};

#[test]
fn example() -> Result<(), PanicOnError> {
    #[derive(Deserialize, Serialize, Debug, PartialEq, Clone)]
    pub struct Distribution {
        pub samples: Vec<f64>,
        pub statistic: String,
    }

    #[derive(Deserialize, Serialize, Debug, PartialEq, Clone)]
    pub struct VectorMetric {
        pub distribution: Option<Distribution>,
    }

    let schema = Schema::from_type::<VectorMetric>(TracingOptions::default())?;
    let fields = schema.to_arrow_fields()?;

    let metrics = [
        VectorMetric {
            distribution: Some(Distribution {
                samples: vec![1.0, 2.0, 3.0],
                statistic: String::from("metric1"),
            }),
        },
        VectorMetric {
            distribution: Some(Distribution {
                samples: vec![4.0, 5.0, 6.0],
                statistic: String::from("metric2"),
            }),
        },
    ];

    let _arrays = serialize_into_arrays(&fields, &metrics)?;
    Ok(())
}

#[derive(Debug)]
struct PanicOnError;

impl<E: std::fmt::Display> From<E> for PanicOnError {
    fn from(value: E) -> Self {
        panic!("{value}");
    }
}
