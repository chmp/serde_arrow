//! Test the error messages from_type is generating

use std::collections::HashMap;

use serde::Deserialize;
use serde_json::json;

use crate::internal::{
    schema::{SchemaLike, SerdeArrowSchema, TracingOptions},
    testing::ResultAsserts,
};

#[test]
fn from_type_budget() {
    let res = SerdeArrowSchema::from_type::<f32>(TracingOptions::default().from_type_budget(0));
    res.assert_error("Could not determine schema from the type after 0 iterations.");
    res.assert_error("Consider increasing the budget option or using `from_samples`.");
}

#[test]
fn non_self_describing_types() {
    let res = SerdeArrowSchema::from_type::<serde_json::Value>(TracingOptions::default());
    res.assert_error("Non self describing types cannot be traced with `from_type`.");
    res.assert_error("Consider using `from_samples`.");
}

#[test]
fn map_as_struct() {
    let res = SerdeArrowSchema::from_type::<HashMap<String, usize>>(
        TracingOptions::default().map_as_struct(true),
    );
    res.assert_error("Cannot trace maps as structs with `from_type`");
    res.assert_error("Consider using `from_samples`.");
}

#[test]
fn outer_struct() {
    let res = SerdeArrowSchema::from_type::<i32>(TracingOptions::default());
    res.assert_error("Only struct-like types are supported as root types in schema tracing.");
    res.assert_error("Consider using the `Item` wrapper,");
}

#[test]
fn enums_without_data() {
    #[derive(Debug, Deserialize)]
    pub enum E {
        A,
        B,
    }

    SerdeArrowSchema::from_type::<E>(TracingOptions::default())
        .assert_error("by setting `enums_without_data_as_strings` to `true`");
}

#[test]
fn missing_overwrites() {
    #[derive(Debug, Deserialize)]
    pub struct S {
        #[allow(dead_code)]
        a: i64,
    }

    SerdeArrowSchema::from_type::<S>(
        TracingOptions::default()
            .overwrite("b", json!({"name": "b", "data_type": "I64"}))
            .unwrap(),
    )
    .assert_error("Overwritten fields could not be found:");
}

#[test]
fn mismatched_overwrite_name() {
    #[derive(Debug, Deserialize)]
    pub struct S {
        #[allow(dead_code)]
        a: i64,
    }

    SerdeArrowSchema::from_type::<S>(
        TracingOptions::default()
            .overwrite("a", json!({"name": "b", "data_type": "I64"}))
            .unwrap(),
    )
    .assert_error("Invalid name for overwritten field");
}

#[test]
fn overwrite_invalid_name() {
    #[derive(Debug, Deserialize)]
    pub struct S {
        #[allow(dead_code)]
        a: i64,
    }

    SerdeArrowSchema::from_type::<S>(
        TracingOptions::default()
            .overwrite("a", json!({"name": "b", "data_type": "I64"}))
            .unwrap(),
    )
    .assert_error("Invalid name for overwritten field \"a\": found \"b\", expected \"a\"");
}
