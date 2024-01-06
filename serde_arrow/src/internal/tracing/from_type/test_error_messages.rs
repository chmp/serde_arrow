//! Test the error messages from_type is generating

use std::collections::HashMap;

use crate::schema::{SchemaLike, SerdeArrowSchema, TracingOptions};

#[test]
fn helpful_error_message_for_budget() {
    let res = SerdeArrowSchema::from_type::<f32>(TracingOptions::default().from_type_budget(0));
    let Err(err) = res else {
        panic!("Expected error, got: {res:?}");
    };
    let err = err.to_string();

    assert!(err.contains("Could not determine schema from the type after 0 iterations."));
    assert!(err.contains("Consider increasing the budget option or using `from_samples`."));
}

#[test]
fn helpful_error_message_for_non_self_describing_types() {
    let res = SerdeArrowSchema::from_type::<serde_json::Value>(TracingOptions::default());
    let Err(err) = res else {
        panic!("Expected error, got: {res:?}");
    };
    let err = err.to_string();

    assert!(err.contains("Non self describing types cannot be traced with `from_type`."));
    assert!(err.contains("Consider using `from_samples`."));
}

#[test]
fn helpful_error_message_for_map_as_struct() {
    let res = SerdeArrowSchema::from_type::<HashMap<String, usize>>(
        TracingOptions::default().map_as_struct(true),
    );
    let Err(err) = res else {
        panic!("Expected error, got: {res:?}");
    };
    let err = err.to_string();

    assert!(err.contains("Cannot trace maps as structs with `from_type`"));
    assert!(err.contains("Consider using `from_samples`."))
}
