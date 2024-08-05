use serde_json::json;

use crate::internal::{
    arrow::{DataType, Field},
    error::PanicOnError,
    schema::{ArrowOrCustomField, STRATEGY_KEY},
    testing::hash_map,
};

use super::serialize::SerializableField;

#[test]
fn i16_field_simple() -> PanicOnError<()> {
    let field = Field {
        name: String::from("my_field_name"),
        data_type: DataType::Int16,
        metadata: hash_map!(),
        nullable: false,
    };
    let expected = json!({
        "name": "my_field_name",
        "data_type": "I16",
    });

    let actual = serde_json::to_value(&SerializableField(&field))?;
    assert_eq!(actual, expected);

    let roundtripped = serde_json::from_value::<ArrowOrCustomField>(actual)?;
    let roundtripped = roundtripped.into_field()?;
    assert_eq!(roundtripped, field);

    Ok(())
}

#[test]
fn date64_field_complex() -> PanicOnError<()> {
    let field = Field {
        name: String::from("my_field_name"),
        data_type: DataType::Date64,
        metadata: hash_map!(
            "foo" => "bar",
            STRATEGY_KEY => "NaiveStrAsDate64",
        ),
        nullable: true,
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

    let roundtripped = serde_json::from_value::<ArrowOrCustomField>(actual)?;
    let roundtripped = roundtripped.into_field()?;
    assert_eq!(roundtripped, field);

    Ok(())
}

#[test]
fn list_field_complex() -> PanicOnError<()> {
    let field = Field {
        name: String::from("my_field_name"),
        data_type: DataType::List(Box::new(Field {
            name: String::from("element"),
            data_type: DataType::Int64,
            metadata: hash_map!(),
            nullable: false,
        })),
        metadata: hash_map!("foo" => "bar"),
        nullable: true,
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

    let roundtripped = serde_json::from_value::<ArrowOrCustomField>(actual)?;
    let roundtripped = roundtripped.into_field()?;
    assert_eq!(roundtripped, field);

    Ok(())
}
