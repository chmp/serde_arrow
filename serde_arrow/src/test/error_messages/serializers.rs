//! Test how errors are reported in serialization, in particular the nesting behavior
use marrow::datatypes::Field;
use serde_json::json;

use crate::internal::{
    error::Error, schema::SchemaLike as _, testing::assert_error_contains, utils::value::Value,
};

fn serialize_to_error(schema: impl serde::Serialize, items: impl serde::Serialize) -> Error {
    let fields = Vec::<Field>::from_value(schema).unwrap();
    crate::to_marrow(&fields, items).unwrap_err()
}

/// Test that errors in top-level structure are reported as-is
#[test]
fn top_level_errors() {
    let err = serialize_to_error(
        json!([{"name": "test", "data_type": "I32"}]),
        Value::FailWithError("test-error"),
    );
    assert_eq!(err.message(), "serde::ser::Error: test-error");

    // TODO: fix this the error should be reported for field $
    let err = serialize_to_error(
        json!([{"name": "test", "data_type": "I32"}]),
        Value::Tuple(vec![Value::FailWithError("test-error")]),
    );
    assert_error_contains(&err, "test-error");
    assert_error_contains(&err, "field: \"$\"");
}

#[test]
fn example() {
    let err = serialize_to_error(
        json!([{"name": "a", "data_type": "I32"}]),
        Value::Tuple(vec![Value::Struct(
            "DummyStruct",
            vec![("a", Value::FailWithError("test-error"))],
        )]),
    );
    assert_error_contains(&err, "field: \"$.a\"");
}

#[test]
fn number_outside_range_unsigned_int() {
    fn test(value: Value) {
        let err = serialize_to_error(
            json!([{"name": "a", "data_type": "U8"}]),
            Value::Tuple(vec![Value::Struct("DummyStruct", vec![("a", value)])]),
        );
        assert_error_contains(&err, "field: \"$.a\"");
    }
    test(Value::I8(-1));
    test(Value::I16(-1));
    test(Value::I32(-1));
    test(Value::I64(-1));
    test(Value::U16(256));
    test(Value::U32(256));
    test(Value::U64(256));
}

mod root_struct {
    use super::*;

    fn schema() -> serde_json::Value {
        json!([{"name": "a", "data_type": "U8"}, {"name": "b", "data_type": "Bool"}])
    }

    /// struct errors are reported on the struct itself
    #[test]
    fn root_struct_direct_errors() {
        let err = serialize_to_error(
            schema(),
            Value::Tuple(vec![Value::FailWithError("struct-error")]),
        );
        assert_error_contains(&err, "struct-error");
        assert_error_contains(&err, "field: \"$\"");
    }

    /// duplicate fields are reported on the struct itself
    #[test]
    fn duplicate_fields() {
        let err = serialize_to_error(
            schema(),
            Value::Tuple(vec![Value::Struct(
                "DummyStruct",
                vec![
                    ("a", Value::U8(0)),
                    ("b", Value::Bool(true)),
                    ("a", Value::U8(0)),
                ],
            )]),
        );
        assert_error_contains(&err, "Duplicate field \"a\"");
        assert_error_contains(&err, "field: \"$\"");
    }

    /// missing fields are reported on the struct itself
    #[test]
    fn missing_fields() {
        let err = serialize_to_error(
            schema(),
            Value::Tuple(vec![Value::Struct(
                "DummyStruct",
                vec![("a", Value::U8(0))],
            )]),
        );
        assert_error_contains(&err, "Missing non-nullable field \"b\"");
        assert_error_contains(&err, "field: \"$\"");
    }

    /// field errors are reported on the field
    #[test]
    fn field_errors_a() {
        let err = serialize_to_error(
            schema(),
            Value::Tuple(vec![Value::Struct(
                "DummyStruct",
                vec![
                    ("a", Value::FailWithError("a-failed")),
                    ("b", Value::Bool(true)),
                ],
            )]),
        );
        assert_error_contains(&err, "a-failed");
        assert_error_contains(&err, "field: \"$.a\"");
    }

    /// field errors are reported on the field
    #[test]
    fn field_errors_b() {
        let err = serialize_to_error(
            schema(),
            Value::Tuple(vec![Value::Struct(
                "DummyStruct",
                vec![("a", Value::U8(0)), ("b", Value::FailWithError("b-failed"))],
            )]),
        );
        assert_error_contains(&err, "b-failed");
        assert_error_contains(&err, "field: \"$.b\"");
    }
}

mod nested_struct {
    use super::*;

    /// The schema
    fn schema() -> serde_json::Value {
        json!([
            {
                "name": "top-level",
                "data_type": "Struct",
                "children": [
                    {"name": "a", "data_type": "U8"},
                    {"name": "b", "data_type": "Bool"},
                ],
            },
        ])
    }

    /// Wrap the top-level struct in a outer list + record
    fn wrap(value: Value) -> Value {
        Value::Tuple(vec![Value::Struct("Record", vec![("top-level", value)])])
    }

    /// struct errors are reported on the struct itself
    #[test]
    fn root_struct_direct_errors() {
        let err = serialize_to_error(schema(), wrap(Value::FailWithError("struct-error")));
        assert_error_contains(&err, "struct-error");
        assert_error_contains(&err, "field: \"$.top-level\"");
    }

    /// duplicate fields are reported on the struct itself
    #[test]
    fn duplicate_fields() {
        let err = serialize_to_error(
            schema(),
            wrap(Value::Struct(
                "TopLevel",
                vec![
                    ("a", Value::U8(0)),
                    ("b", Value::Bool(true)),
                    ("a", Value::U8(0)),
                ],
            )),
        );
        assert_error_contains(&err, "Duplicate field \"a\"");
        assert_error_contains(&err, "field: \"$.top-level\"");
    }

    /// missing fields are reported on the struct itself
    #[test]
    fn missing_fields() {
        let err = serialize_to_error(
            schema(),
            wrap(Value::Struct("TopLevel", vec![("a", Value::U8(0))])),
        );
        assert_error_contains(&err, "Missing non-nullable field \"b\"");
        assert_error_contains(&err, "field: \"$.top-level\"");
    }

    /// field errors are reported on the field
    #[test]
    fn field_errors_a() {
        let err = serialize_to_error(
            schema(),
            wrap(Value::Struct(
                "TopLevel",
                vec![
                    ("a", Value::FailWithError("a-failed")),
                    ("b", Value::Bool(true)),
                ],
            )),
        );
        assert_error_contains(&err, "a-failed");
        assert_error_contains(&err, "field: \"$.top-level.a\"");
    }

    /// field errors are reported on the field
    #[test]
    fn field_errors_b() {
        let err = serialize_to_error(
            schema(),
            wrap(Value::Struct(
                "TopLevel",
                vec![("a", Value::U8(0)), ("b", Value::FailWithError("b-failed"))],
            )),
        );
        assert_error_contains(&err, "b-failed");
        assert_error_contains(&err, "field: \"$.top-level.b\"");
    }
}

mod unions {
    use crate::internal::utils::value::Variant;

    use super::*;

    /// The schema under test
    fn schema() -> serde_json::Value {
        json!([
            {
                "name": "union",
                "data_type": "Union",
                "children": [
                    {"name": "a", "data_type": "UInt8"},
                    {
                        "name": "b",
                        "data_type":
                        "Struct", "children": [
                            {"name": "0", "data_type": "UInt16"},
                            {"name": "1", "data_type": "UInt32"},
                        ],
                    },
                    {
                        "name": "c",
                        "data_type": "List",
                        "children": [
                            {"name": "element", "data_type": "Bool"},
                        ],
                    }
                ],
            }
        ])
    }

    /// Wrap the top-level union value in a tuple + record
    fn wrap(value: Value) -> Value {
        Value::Tuple(vec![Value::Struct("Record", vec![("union", value)])])
    }

    const VARIANT_A: Variant = Variant("Union", 0, "a");
    const VARIANT_B: Variant = Variant("Union", 1, "b");
    const VARIANT_C: Variant = Variant("Union", 2, "c");

    /// Variant level errors do not report the variant
    #[test]
    fn variant_level_errors() {
        let err = serialize_to_error(
            schema(),
            wrap(Value::NewtypeVariant(
                VARIANT_A,
                Box::new(Value::FailWithError("test-error")),
            )),
        );

        assert_error_contains(&err, "test-error");
        assert_error_contains(&err, "field: \"$.union\"");
    }

    #[test]
    fn nested_struct_errors() {
        let err = serialize_to_error(
            schema(),
            wrap(Value::StructVariant(
                VARIANT_B,
                vec![
                    ("0", Value::U16(0)),
                    ("1", Value::FailWithError("test-error")),
                ],
            )),
        );
        assert_error_contains(&err, "test-error");
        assert_error_contains(&err, "field: \"$.union.1\"");
    }

    #[test]
    fn top_level_error_in_list() {
        let err = serialize_to_error(
            schema(),
            wrap(Value::NewtypeVariant(
                VARIANT_C,
                Box::new(Value::FailWithError("test-error")),
            )),
        );
        assert_error_contains(&err, "test-error");
        assert_error_contains(&err, "field: \"$.union\"");
    }

    #[test]
    fn nested_element_error_in_list() {
        let err = serialize_to_error(
            schema(),
            wrap(Value::TupleVariant(
                VARIANT_C,
                vec![Value::FailWithError("test-error")],
            )),
        );
        assert_error_contains(&err, "test-error");
        assert_error_contains(&err, "field: \"$.union.element\"");
    }

    #[test]
    fn invalid_type() {
        let err = serialize_to_error(
            schema(),
            wrap(Value::NewtypeVariant(VARIANT_B, Box::new(Value::U16(0)))),
        );
        assert_error_contains(&err, "StructBuilder does not support serialize_u16");
        assert_error_contains(&err, "field: \"$.union\"");
    }
}

mod lists {
    use super::*;

    /// the schema under test
    fn schema(data_type: &str) -> serde_json::Value {
        json!([
            {
                "name": "list",
                "data_type": data_type,
                "children": [
                    { "name": "element", "data_type": "U8" },
                ],
            }
        ])
    }

    /// Wrap the list in a tuple + record
    fn wrap(value: Value) -> Value {
        Value::Tuple(vec![Value::Struct("Record", vec![("list", value)])])
    }

    #[test]
    fn top_level_errors_list() {
        fn test(data_type: &str) {
            let err =
                serialize_to_error(schema(data_type), wrap(Value::FailWithError("test-error")));
            assert_error_contains(&err, "test-error");
            assert_error_contains(&err, "field: \"$.list\"");
            assert_error_contains(&err, &format!("data_type: \"{data_type}\""));
        }
        test("List");
        test("LargeList");
        test("FixedSizeList(2)");
    }

    #[test]
    fn element_errors() {
        fn test(data_type: &str) {
            let err = serialize_to_error(
                schema(data_type),
                wrap(Value::Seq(vec![Value::FailWithError("test-error")])),
            );
            assert_error_contains(&err, "test-error");
            assert_error_contains(&err, "field: \"$.list.element\"");
            assert_error_contains(&err, "data_type: \"UInt8\"");
        }
        test("List");
        test("LargeList");
        test("FixedSizeList(2)");
    }

    #[test]
    fn fixed_size_list_too_may_items() {
        let err = serialize_to_error(
            schema("FixedSizeList(2)"),
            wrap(Value::Seq(vec![
                Value::U8(0),
                Value::U8(1),
                Value::U8(2),
                Value::U8(3),
            ])),
        );
        assert_error_contains(&err, "Invalid number of elements");
        assert_error_contains(&err, "field: \"$.list\"");
        assert_error_contains(&err, "data_type: \"FixedSizeList(2)\"");
    }

    #[test]
    fn fixed_size_list_too_little_items() {
        let err = serialize_to_error(
            schema("FixedSizeList(2)"),
            wrap(Value::Seq(vec![Value::U8(0)])),
        );
        assert_error_contains(&err, "Invalid number of elements");
        assert_error_contains(&err, "field: \"$.list\"");
        assert_error_contains(&err, "data_type: \"FixedSizeList(2)\"");
    }
}
