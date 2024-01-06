use crate::schema::{SchemaLike, SerdeArrowSchema, TracingOptions};

#[test]
fn outer_struct() {
    let res = SerdeArrowSchema::from_samples(&[1_u32, 2_u32, 3_u32], TracingOptions::default());
    let Err(err) = res else {
        panic!("Expected error, got: {res:?}");
    };
    let err = err.to_string();

    assert!(err.contains("Only struct-like types are supported as root types in schema tracing."));
    assert!(err.contains("Consider using the `Items` wrapper,"));
}
