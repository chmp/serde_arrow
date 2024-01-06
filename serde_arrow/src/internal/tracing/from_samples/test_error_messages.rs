use crate::schema::{SchemaLike, SerdeArrowSchema, TracingOptions};

#[test]
fn outer_struct() {
    let res = SerdeArrowSchema::from_samples(&[1_u32, 2_u32, 3_u32], TracingOptions::default());
    let Err(err) = res else {
        panic!("Expected error, got: {res:?}");
    };
    let err = err.to_string();

    println!("Error message: {err}");
    assert!(err.contains("Only struct-like types are supported as root types in schema tracing."));
    assert!(err.contains("Consider using the `Items` wrapper,"));
}

/// See: https://github.com/chmp/serde_arrow/issues/97
#[test]
fn outer_sequence_issue_97() {
    use serde::Serialize;

    #[derive(Debug, Serialize)]
    pub struct A {
        pub b: String,
        pub k: f64,
    }
    let b = A {
        b: String::from("Test"),
        k: 100.0,
    };

    let res = SerdeArrowSchema::from_samples(&b, TracingOptions::default());
    let Err(err) = res else {
        panic!("Expected error, got: {res:?}");
    };
    let err = err.to_string();

    println!("Error message: {err}");
    assert!(err.contains("Cannot trace non-sequences with `from_samples`."));
    assert!(err.contains("Consider wrapping the argument in an array."));
}
