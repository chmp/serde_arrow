use serde::Deserialize;
use serde_json::json;

use crate::internal::{
    arrow::{ArrayView, DataType, Field, PrimitiveArrayView},
    deserializer::Deserializer,
    utils::{Item, Items},
};

use super::utils::Test;

#[test]
fn bool_as_int8() {
    let items = &[Item(true), Item(false)];
    Test::new()
        .with_schema(json!([{"name": "item", "data_type": "U8"}]))
        .serialize(items)
        .deserialize(items)
        .check_nulls(&[&[false, false]]);
}

#[test]
fn nullable_bool_as_int8() {
    let items = &[Item(Some(true)), Item(None), Item(Some(false))];
    Test::new()
        .with_schema(json!([{"name": "item", "data_type": "U8", "nullable": true}]))
        .serialize(items)
        .deserialize(items)
        .check_nulls(&[&[false, true, false]]);
}

// from the bool8 specs: false is denoted by the value 0. true can be specified using any non-zero
// value. Preferably 1.
#[test]
fn deserialize_from_not_01_ints() -> crate::internal::error::PanicOnError<()> {
    let field = Field {
        name: String::from("item"),
        data_type: DataType::Int8,
        nullable: false,
        metadata: Default::default(),
    };
    let view = ArrayView::Int8(PrimitiveArrayView {
        validity: None,
        values: &[0, -1, 2, 3, -31, 100, 0, 0],
    });
    let deserializer = Deserializer::new(&[field], vec![view])?;

    let Items(actual) = Items::<Vec<bool>>::deserialize(deserializer)?;
    let expected = vec![false, true, true, true, true, true, false, false];
    assert_eq!(actual, expected);

    Ok(())
}
