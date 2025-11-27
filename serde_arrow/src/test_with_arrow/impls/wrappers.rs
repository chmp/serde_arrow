use super::utils::Test;

/// Test against supported Rust types
mod rust_types {
    use super::*;

    use crate::internal::{schema::TracingOptions, utils::Items};

    #[test]
    fn outer_vec() {
        let items: Vec<u32> = vec![0_u32, 1_u32, 2_u32];
        Test::new()
            .trace_schema_from_samples(&Items(&items), TracingOptions::default())
            .serialize(&Items(&items));
    }

    #[test]
    fn outer_slice() {
        let items: &[u32] = &[0_u32, 1_u32, 2_u32];
        Test::new()
            .trace_schema_from_samples(&Items(items), TracingOptions::default())
            .serialize(&Items(items));
    }

    #[test]
    fn outer_array() {
        let items: &[u32; 3] = &[0_u32, 1_u32, 2_u32];
        Test::new()
            .trace_schema_from_samples(&Items(items), TracingOptions::default())
            .serialize(&Items(items));
    }

    #[test]
    fn outer_tuple() {
        // Note: the standard Items wrapper does not work with tuples, use a custom impl here
        #[derive(serde::Serialize)]
        struct Item {
            item: u32,
        }

        let items: &(Item, Item, Item) = &(
            Item { item: 0_u32 },
            Item { item: 1_u32 },
            Item { item: 2_u32 },
        );

        Test::new()
            .trace_schema_from_samples(items, TracingOptions::default())
            .serialize(items);
    }
}

/// Test directly against the serde data model
mod serde_data_model {
    use super::*;

    use crate::internal::utils::value::{Value, Variant};

    use serde_json::json;

    fn item() -> Value {
        Value::Struct("Record", vec![("a", Value::U8(0))])
    }

    #[test]
    fn seq() {
        Test::new()
            .with_schema(json!([{"name": "a", "data_type": "UInt8"}]))
            .serialize(&Value::Seq(vec![item()]));
    }

    #[test]
    fn tuple() {
        Test::new()
            .with_schema(json!([{"name": "a", "data_type": "UInt8"}]))
            .serialize(&Value::Tuple(vec![item()]));
    }

    #[test]
    fn tuple_struct() {
        Test::new()
            .with_schema(json!([{"name": "a", "data_type": "UInt8"}]))
            .serialize(&Value::TupleStruct("Wrapper", vec![item()]));
    }

    #[test]
    fn tuple_variant() {
        Test::new()
            .with_schema(json!([{"name": "a", "data_type": "UInt8"}]))
            .serialize(&Value::TupleVariant(
                Variant("Tuple", 0, "Variant"),
                vec![item()],
            ));
    }

    #[test]
    fn newtype_wrapper() {
        Test::new()
            .with_schema(json!([{"name": "a", "data_type": "UInt8"}]))
            .serialize(&Value::NewtypeStruct(
                "Wrappper",
                Box::new(Value::Seq(vec![item()])),
            ));
    }

    #[test]
    fn newtype_variant_wrapper() {
        Test::new()
            .with_schema(json!([{"name": "a", "data_type": "UInt8"}]))
            .serialize(&Value::NewtypeVariant(
                Variant("Wrappper", 0, "Variant"),
                Box::new(Value::Seq(vec![item()])),
            ));
    }
}
