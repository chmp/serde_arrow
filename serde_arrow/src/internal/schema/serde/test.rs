use serde_json::json;

use crate::internal::{
    error::PanicOnError,
    schema::{GenericDataType, GenericField, STRATEGY_KEY},
    testing::hash_map,
};

#[test]
fn i16_field_simple() -> PanicOnError<()> {
    let field = GenericField {
        name: String::from("my_field_name"),
        data_type: GenericDataType::I16,
        metadata: hash_map!(),
        nullable: false,
        children: vec![],
    };
    let expected = json!({
        "name": "my_field_name",
        "data_type": "I16",
    });

    let actual = serde_json::to_value(&field)?;
    assert_eq!(actual, expected);

    let roundtripped = serde_json::from_value::<GenericField>(actual)?;
    assert_eq!(roundtripped, field);

    Ok(())
}

#[test]
fn date64_field_complex() -> PanicOnError<()> {
    let field = GenericField {
        name: String::from("my_field_name"),
        data_type: GenericDataType::Date64,
        metadata: hash_map!(
            "foo" => "bar",
            STRATEGY_KEY => "NaiveStrAsDate64",
        ),
        nullable: true,
        children: vec![],
    };
    let expected = json!({
        "name": "my_field_name",
        "data_type": "Date64",
        "metadata": {
            "foo": "bar",
        },
        "strategy": "NaiveStrAsDate64",
        "nullable": true,
    });

    let actual = serde_json::to_value(&field)?;
    assert_eq!(actual, expected);

    let roundtripped = serde_json::from_value::<GenericField>(actual)?;
    assert_eq!(roundtripped, field);

    Ok(())
}

#[test]
fn list_field_complex() -> PanicOnError<()> {
    let field = GenericField {
        name: String::from("my_field_name"),
        data_type: GenericDataType::List,
        metadata: hash_map!("foo" => "bar"),
        nullable: true,
        children: vec![GenericField {
            name: String::from("element"),
            data_type: GenericDataType::I64,
            metadata: hash_map!(),
            nullable: false,
            children: vec![],
        }],
    };
    let expected = json!({
        "name": "my_field_name",
        "data_type": "List",
        "metadata": {"foo": "bar"},
        "nullable": true,
        "children": [
            {"name": "element", "data_type": "I64"},
        ]
    });

    let actual = serde_json::to_value(&field)?;
    assert_eq!(actual, expected);

    let roundtripped = serde_json::from_value::<GenericField>(actual)?;
    assert_eq!(roundtripped, field);

    Ok(())
}
