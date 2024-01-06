//! Test the error messages from_type is generating

use crate::schema::{SchemaLike, SerdeArrowSchema, TracingOptions};

#[test]
fn helpful_error_message_for_budget() {
    let res = SerdeArrowSchema::from_type::<f32>(TracingOptions::default().from_type_budget(0));
    let Err(err) = res else {
        panic!("Expected error, got: {res:?}");
    };

    assert!(err
        .to_string()
        .contains("Could not determine schema from the type after 0 iterations."));
}

#[test]
fn helpful_error_message_for_non_self_describing_types() {
    let res = SerdeArrowSchema::from_type::<serde_json::Value>(TracingOptions::default());
    let Err(err) = res else {
        panic!("Expected error, got: {res:?}");
    };

    assert!(err.to_string().contains("deserialize_any"));
}
