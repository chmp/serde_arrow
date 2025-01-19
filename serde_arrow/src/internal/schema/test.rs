use std::collections::HashMap;

use marrow::datatypes::{DataType, Field, TimeUnit};

use serde_json::{json, Value};

use crate::internal::{
    error::PanicOnError,
    schema::{SchemaLike, SerdeArrowSchema, STRATEGY_KEY},
    testing::{assert_error_contains, hash_map},
};

fn type_from_str(s: &str) -> DataType {
    let schema = SerdeArrowSchema::from_value(&json!([{"name": "item", "data_type": s}])).unwrap();
    schema.fields[0].data_type.clone()
}

fn pretty_str_from_type(data_type: &DataType) -> String {
    let schema = SerdeArrowSchema {
        fields: vec![Field {
            name: String::from("item"),
            data_type: data_type.clone(),
            nullable: false,
            metadata: Default::default(),
        }],
    };
    let json = serde_json::to_value(schema).unwrap();

    let Value::String(data_type) = json
        .get("fields")
        .unwrap()
        .get(0)
        .unwrap()
        .get("data_type")
        .unwrap()
    else {
        panic!("data type must be string");
    };
    data_type.clone()
}

#[test]
fn i16_field_simple() -> PanicOnError<()> {
    let schema = SerdeArrowSchema {
        fields: vec![Field {
            name: String::from("my_field_name"),
            data_type: DataType::Int16,
            metadata: hash_map!(),
            nullable: false,
        }],
    };
    let expected = json!({
        "fields": [
            {
                "name": "my_field_name",
                "data_type": "I16",
            }
        ],
    });

    let actual = serde_json::to_value(&schema)?;
    assert_eq!(actual, expected);

    let roundtripped = SerdeArrowSchema::from_value(&actual)?;
    assert_eq!(roundtripped, schema);

    Ok(())
}

#[test]
fn date64_field_complex() -> PanicOnError<()> {
    let schema = SerdeArrowSchema {
        fields: vec![Field {
            name: String::from("my_field_name"),
            data_type: DataType::Date64,
            metadata: hash_map!(
                "foo" => "bar",
                STRATEGY_KEY => "NaiveStrAsDate64",
            ),
            nullable: true,
        }],
    };
    let expected = json!({
        "fields": [{
            "name": "my_field_name",
            "data_type": "Date64",
            "metadata": {
                "foo": "bar",
            },
            "strategy": "NaiveStrAsDate64",
            "nullable": true,
        }],
    });

    let actual = serde_json::to_value(&schema)?;
    assert_eq!(actual, expected);

    let roundtripped = SerdeArrowSchema::from_value(&actual)?;
    assert_eq!(roundtripped, schema);

    Ok(())
}

#[test]
fn list_field_complex() -> PanicOnError<()> {
    let schema = SerdeArrowSchema {
        fields: vec![Field {
            name: String::from("my_field_name"),
            data_type: DataType::List(Box::new(Field {
                name: String::from("element"),
                data_type: DataType::Int64,
                metadata: hash_map!(),
                nullable: false,
            })),
            metadata: hash_map!("foo" => "bar"),
            nullable: true,
        }],
    };
    let expected = json!({
        "fields": [{
            "name": "my_field_name",
            "data_type": "List",
            "metadata": {"foo": "bar"},
            "nullable": true,
            "children": [
                {"name": "element", "data_type": "I64"},
            ],
        }],
    });

    let actual = serde_json::to_value(&schema)?;
    assert_eq!(actual, expected);

    let roundtripped = SerdeArrowSchema::from_value(&actual)?;
    assert_eq!(roundtripped, schema);

    Ok(())
}

#[test]
fn map_field_complex() -> PanicOnError<()> {
    let schema = SerdeArrowSchema {
        fields: vec![Field {
            name: String::from("my_field_name"),
            metadata: Default::default(),
            nullable: false,
            data_type: DataType::Map(
                Box::new(Field {
                    name: String::from("entry"),
                    data_type: DataType::Struct(vec![
                        Field {
                            name: String::from("key"),
                            data_type: DataType::Utf8,
                            nullable: false,
                            metadata: Default::default(),
                        },
                        Field {
                            name: String::from("value"),
                            data_type: DataType::Int32,
                            nullable: false,
                            metadata: Default::default(),
                        },
                    ]),
                    metadata: Default::default(),
                    nullable: false,
                }),
                false,
            ),
        }],
    };
    let expected = json!({
        "fields": [{
            "name": "my_field_name",
            "data_type": "Map",
            "children": [
                {
                    "name": "entry",
                    "data_type": "Struct",
                    "children": [
                        {"name": "key", "data_type": "Utf8"},
                        {"name": "value", "data_type": "I32"},
                    ]
                },
            ],
        }],
    });

    let actual = serde_json::to_value(&schema)?;
    assert_eq!(actual, expected);

    let roundtripped = SerdeArrowSchema::from_value(&actual)?;
    assert_eq!(roundtripped, schema);

    Ok(())
}

#[test]
fn null_fields_are_nullable_implicitly() -> PanicOnError<()> {
    let expected = SerdeArrowSchema {
        fields: vec![Field {
            name: String::from("item"),
            data_type: DataType::Null,
            metadata: hash_map!(),
            nullable: true,
        }],
    };
    let schema = json!({
        "fields": [
            {
                "name": "item",
                "data_type": "Null",
            }
        ],
    });

    let actual = SerdeArrowSchema::from_value(&schema)?;
    assert_eq!(actual, expected);

    Ok(())
}

#[test]
fn example() {
    let schema = SerdeArrowSchema {
        fields: vec![
            Field {
                name: String::from("foo"),
                data_type: DataType::UInt8,
                nullable: false,
                metadata: HashMap::new(),
            },
            Field {
                name: String::from("bar"),
                data_type: DataType::Utf8,
                nullable: false,
                metadata: Default::default(),
            },
        ],
    };

    let actual = serde_json::to_string(&schema).unwrap();
    assert_eq!(
        actual,
        r#"{"fields":[{"name":"foo","data_type":"U8"},{"name":"bar","data_type":"Utf8"}]}"#
    );

    let round_tripped: SerdeArrowSchema = serde_json::from_str(&actual).unwrap();
    assert_eq!(round_tripped, schema);
}

#[test]
fn example_without_wrapper() {
    let expected = SerdeArrowSchema {
        fields: vec![
            Field {
                name: String::from("foo"),
                data_type: DataType::UInt8,
                nullable: false,
                metadata: HashMap::new(),
            },
            Field {
                name: String::from("bar"),
                data_type: DataType::Utf8,
                nullable: false,
                metadata: Default::default(),
            },
        ],
    };

    let input = r#"[{"name":"foo","data_type":"U8"},{"name":"bar","data_type":"Utf8"}]"#;
    let actual: SerdeArrowSchema = serde_json::from_str(&input).unwrap();
    assert_eq!(actual, expected);
}

#[test]
fn list() {
    let schema = SerdeArrowSchema {
        fields: vec![Field {
            name: String::from("value"),
            data_type: DataType::List(Box::new(Field {
                name: String::from("element"),
                data_type: DataType::Int32,
                nullable: false,
                metadata: Default::default(),
            })),
            nullable: false,
            metadata: Default::default(),
        }],
    };

    let actual = serde_json::to_string(&schema).unwrap();
    assert_eq!(
        actual,
        r#"{"fields":[{"name":"value","data_type":"List","children":[{"name":"element","data_type":"I32"}]}]}"#
    );

    let round_tripped: SerdeArrowSchema = serde_json::from_str(&actual).unwrap();
    assert_eq!(round_tripped, schema);
}

#[test]
fn doc_schema() {
    let schema = r#"
        [
            {"name":"foo","data_type":"U8"},
            {"name":"bar","data_type":"Utf8"}
        ]
    "#;

    let actual: SerdeArrowSchema = serde_json::from_str(&schema).unwrap();

    let expected = SerdeArrowSchema {
        fields: vec![
            Field {
                name: String::from("foo"),
                data_type: DataType::UInt8,
                nullable: false,
                metadata: HashMap::new(),
            },
            Field {
                name: String::from("bar"),
                data_type: DataType::Utf8,
                nullable: false,
                metadata: Default::default(),
            },
        ],
    };

    assert_eq!(actual, expected);
}

#[test]
fn test_metadata_strategy_from_explicit() {
    let schema = SerdeArrowSchema::from_value(&json!([
        {
            "name": "example",
            "data_type": "Date64",
            "strategy": "UtcStrAsDate64",
            "metadata": {
                "foo": "bar",
                "hello": "world",
            },
        },
    ]))
    .unwrap();

    assert_eq!(
        schema.fields[0].metadata,
        hash_map!("foo" => "bar", "hello" => "world", STRATEGY_KEY => "UtcStrAsDate64"),
    );

    let schema_value = serde_json::to_value(&schema).unwrap();
    let expected_schema_value = json!({
        "fields": [
            {
                "name": "example",
                "data_type": "Date64",
                "strategy": "UtcStrAsDate64",
                "metadata": {
                    "foo": "bar",
                    "hello": "world",
                },
            },
        ],
    });

    assert_eq!(schema_value, expected_schema_value);
}

#[test]
fn test_metadata_strategy_from_metadata() {
    let schema = SerdeArrowSchema::from_value(&json!([
        {
            "name": "example",
            "data_type": "Date64",
            "metadata": {
                STRATEGY_KEY: "UtcStrAsDate64",
                "foo": "bar",
                "hello": "world",
            },
        },
    ]))
    .unwrap();

    assert_eq!(
        schema.fields[0].metadata,
        hash_map!("foo" => "bar", "hello" => "world", STRATEGY_KEY => "UtcStrAsDate64")
    );

    // NOTE: the strategy is always normalized to be an extra field
    let schema_value = serde_json::to_value(&schema).unwrap();
    let expected_schema_value = json!({
        "fields": [
            {
                "name": "example",
                "data_type": "Date64",
                "strategy": "UtcStrAsDate64",
                "metadata": {
                    "foo": "bar",
                    "hello": "world",
                },
            },
        ],
    });

    assert_eq!(schema_value, expected_schema_value);
}

#[test]
fn test_invalid_metadata() {
    // strategies cannot be given both in metadata and strategy field
    let res = SerdeArrowSchema::from_value(&json!([
        {
            "name": "example",
            "data_type": "Date64",
            "strategy": "UtcStrAsDate64",
            "metadata": {
                STRATEGY_KEY: "UtcStrAsDate64"
            },
        },
    ]));

    assert_error_contains(&res, "Duplicate strategy");
}

#[test]
fn test_long_form_types() {
    assert_eq!(type_from_str("Boolean"), DataType::Boolean);
    assert_eq!(type_from_str("Int8"), DataType::Int8);
    assert_eq!(type_from_str("Int16"), DataType::Int16);
    assert_eq!(type_from_str("Int32"), DataType::Int32);
    assert_eq!(type_from_str("Int64"), DataType::Int64);
    assert_eq!(type_from_str("UInt8"), DataType::UInt8);
    assert_eq!(type_from_str("UInt16"), DataType::UInt16);
    assert_eq!(type_from_str("UInt32"), DataType::UInt32);
    assert_eq!(type_from_str("UInt64"), DataType::UInt64);
    assert_eq!(type_from_str("Float16"), DataType::Float16);
    assert_eq!(type_from_str("Float32"), DataType::Float32);
    assert_eq!(type_from_str("Float64"), DataType::Float64);
    assert_eq!(
        type_from_str("Decimal128(8,-2)"),
        DataType::Decimal128(8, -2)
    );
    assert_eq!(
        type_from_str("Decimal128( 8 , -2 )"),
        DataType::Decimal128(8, -2)
    );
}

macro_rules! test_short_form_type {
    ($name:ident, $data_type:expr, $s:expr) => {
        #[test]
        fn $name() {
            let data_type: DataType = $data_type;
            let s: &str = $s;
            assert_eq!(pretty_str_from_type(&data_type), s);
            assert_eq!(type_from_str(s), data_type);
        }
    };
}

test_short_form_type!(test_null, DataType::Null, "Null");
test_short_form_type!(test_boolean, DataType::Boolean, "Bool");
test_short_form_type!(test_int8, DataType::Int8, "I8");
test_short_form_type!(test_int16, DataType::Int16, "I16");
test_short_form_type!(test_int32, DataType::Int32, "I32");
test_short_form_type!(test_int64, DataType::Int64, "I64");
test_short_form_type!(test_uint8, DataType::UInt8, "U8");
test_short_form_type!(test_uint16, DataType::UInt16, "U16");
test_short_form_type!(test_uint32, DataType::UInt32, "U32");
test_short_form_type!(test_uint64, DataType::UInt64, "U64");
test_short_form_type!(test_float16, DataType::Float16, "F16");
test_short_form_type!(test_float32, DataType::Float32, "F32");
test_short_form_type!(test_float64, DataType::Float64, "F64");
test_short_form_type!(test_date_32, DataType::Date32, "Date32");
test_short_form_type!(test_date_64, DataType::Date64, "Date64");

test_short_form_type!(test_utf8, DataType::Utf8, "Utf8");
test_short_form_type!(test_large_utf8, DataType::LargeUtf8, "LargeUtf8");

test_short_form_type!(test_binary, DataType::Binary, "Binary");
test_short_form_type!(test_large_binary, DataType::LargeBinary, "LargeBinary");

test_short_form_type!(
    test_fixed_size_binary,
    DataType::FixedSizeBinary(32),
    "FixedSizeBinary(32)"
);
test_short_form_type!(
    test_decimal_128,
    DataType::Decimal128(2, -2),
    "Decimal128(2, -2)"
);

test_short_form_type!(
    test_timestamp_no_tz,
    DataType::Timestamp(TimeUnit::Second, None),
    "Timestamp(Second, None)"
);
test_short_form_type!(
    test_timestamp_utc,
    DataType::Timestamp(TimeUnit::Millisecond, Some(String::from("Utc"))),
    "Timestamp(Millisecond, Some(\"Utc\"))"
);

test_short_form_type!(
    test_time32_second,
    DataType::Time32(TimeUnit::Second),
    "Time32(Second)"
);
test_short_form_type!(
    test_time32_millisecond,
    DataType::Time32(TimeUnit::Millisecond),
    "Time32(Millisecond)"
);

test_short_form_type!(
    test_time64_microsecond,
    DataType::Time64(TimeUnit::Microsecond),
    "Time64(Microsecond)"
);
test_short_form_type!(
    test_time64_nanosecond,
    DataType::Time64(TimeUnit::Nanosecond),
    "Time64(Nanosecond)"
);

test_short_form_type!(
    test_duration_second,
    DataType::Duration(TimeUnit::Second),
    "Duration(Second)"
);
test_short_form_type!(
    test_duration_nanosecond,
    DataType::Duration(TimeUnit::Nanosecond),
    "Duration(Nanosecond)"
);
