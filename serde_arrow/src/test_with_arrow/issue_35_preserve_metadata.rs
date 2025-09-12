//! Test that the meta data is correctly preserved
use std::collections::HashMap;

use marrow::datatypes::Field;
use serde_json::json;

use crate::{
    _impl::{arrow, arrow2},
    internal::{
        schema::{transmute_field, SchemaLike, SerdeArrowSchema, STRATEGY_KEY},
        testing::hash_map,
    },
};

fn example_field_desc() -> serde_json::Value {
    json!({
        "name": "example",
        "data_type": "Struct",
        "strategy": "MapAsStruct",
        "metadata": {"foo": "bar"},
        "children": [
            {
                "name": "first",
                "data_type": "I32",
            },
            {
                "name": "second",
                "data_type": "I64",
            },
        ],
    })
}

#[test]
fn arrow() {
    let initial_field = transmute_field(example_field_desc()).unwrap();
    assert_eq!(
        initial_field.metadata,
        hash_map!("foo" => "bar", STRATEGY_KEY => "MapAsStruct")
    );

    let arrow_field = arrow::datatypes::Field::try_from(&initial_field).unwrap();
    assert_eq!(
        arrow_field.metadata(),
        &hash_map!("foo" => "bar", STRATEGY_KEY => "MapAsStruct")
    );

    // roundtrip via try_from
    let generic_field = Field::try_from(&arrow_field).unwrap();
    assert_eq!(generic_field, initial_field);

    // roundtrip via serialize
    let schema = SerdeArrowSchema::from_value(vec![arrow_field]).unwrap();
    assert_eq!(schema.fields.len(), 1);
    assert_eq!(schema.fields[0], initial_field);
}

#[test]
fn arrow2() {
    let initial_field = transmute_field(example_field_desc()).unwrap();
    assert_eq!(
        initial_field.metadata,
        hash_map!("foo" => "bar", STRATEGY_KEY => "MapAsStruct")
    );

    let arrow_field = arrow2::datatypes::Field::try_from(&initial_field).unwrap();
    assert_eq!(
        arrow_field
            .metadata
            .clone()
            .into_iter()
            .collect::<HashMap<_, _>>(),
        hash_map!("foo" => "bar", STRATEGY_KEY => "MapAsStruct")
    );

    // roundtrip via try_from
    let generic_field = Field::try_from(&arrow_field).unwrap();
    assert_eq!(generic_field, initial_field);

    // note: arrow2 Field does not support serialize
}
