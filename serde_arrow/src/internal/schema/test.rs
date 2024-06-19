use serde_json::json;

use crate::internal::{
    schema::{GenericDataType, GenericField, SchemaLike, SerdeArrowSchema, Strategy, STRATEGY_KEY},
    testing::{assert_error, hash_map},
};

impl SerdeArrowSchema {
    fn with_field(mut self, field: GenericField) -> Self {
        self.fields.push(field);
        self
    }
}

#[test]
fn example() {
    let schema = SerdeArrowSchema::default()
        .with_field(GenericField::new("foo", GenericDataType::U8, false))
        .with_field(GenericField::new("bar", GenericDataType::Utf8, false));

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
    let expected = SerdeArrowSchema::default()
        .with_field(GenericField::new("foo", GenericDataType::U8, false))
        .with_field(GenericField::new("bar", GenericDataType::Utf8, false));

    let input = r#"[{"name":"foo","data_type":"U8"},{"name":"bar","data_type":"Utf8"}]"#;
    let actual: SerdeArrowSchema = serde_json::from_str(&input).unwrap();
    assert_eq!(actual, expected);
}

#[test]
fn list() {
    let schema = SerdeArrowSchema::default().with_field(
        GenericField::new("value", GenericDataType::List, false).with_child(GenericField::new(
            "element",
            GenericDataType::I32,
            false,
        )),
    );

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
    let expected = SerdeArrowSchema::default()
        .with_field(GenericField::new("foo", GenericDataType::U8, false))
        .with_field(GenericField::new("bar", GenericDataType::Utf8, false));

    assert_eq!(actual, expected);
}

#[test]
fn date64_with_strategy() {
    let schema = SerdeArrowSchema::default().with_field(
        GenericField::new("item", GenericDataType::Date64, false)
            .with_strategy(Strategy::NaiveStrAsDate64),
    );

    let actual = serde_json::to_string(&schema).unwrap();
    assert_eq!(
        actual,
        r#"{"fields":[{"name":"item","data_type":"Date64","strategy":"NaiveStrAsDate64"}]}"#
    );

    let round_tripped: SerdeArrowSchema = serde_json::from_str(&actual).unwrap();
    assert_eq!(round_tripped, schema);

    let json = serde_json::json!([{
        "name": "item",
        "data_type": "Date64",
        "strategy": "NaiveStrAsDate64",
    }]);

    let from_json = SerdeArrowSchema::from_value(&json).unwrap();
    assert_eq!(from_json, schema);
}

#[test]
fn timestamp_second_serialization() {
    let dt = super::GenericDataType::Timestamp(super::GenericTimeUnit::Second, None);

    let s = serde_json::to_string(&dt).unwrap();
    assert_eq!(s, r#""Timestamp(Second, None)""#);

    let rt = serde_json::from_str(&s).unwrap();
    assert_eq!(dt, rt);
}

#[test]
fn timestamp_second_utc_serialization() {
    let dt = super::GenericDataType::Timestamp(
        super::GenericTimeUnit::Second,
        Some(String::from("Utc")),
    );

    let s = serde_json::to_string(&dt).unwrap();
    assert_eq!(s, r#""Timestamp(Second, Some(\"Utc\"))""#);

    let rt = serde_json::from_str(&s).unwrap();
    assert_eq!(dt, rt);
}

#[test]
fn test_date32() {
    use super::GenericDataType as DT;

    assert_eq!(DT::Date32.to_string(), "Date32");
    assert_eq!("Date32".parse::<DT>().unwrap(), DT::Date32);
}

#[test]
fn time64_data_type_format() {
    use super::{GenericDataType as DT, GenericTimeUnit as TU};

    for (dt, s) in [
        (DT::Time64(TU::Microsecond), "Time64(Microsecond)"),
        (DT::Time64(TU::Nanosecond), "Time64(Nanosecond)"),
    ] {
        assert_eq!(dt.to_string(), s);
        assert_eq!(s.parse::<DT>().unwrap(), dt);
    }
}

#[test]
fn test_long_form_types() {
    use super::GenericDataType as DT;
    use std::str::FromStr;

    assert_eq!(DT::from_str("Boolean").unwrap(), DT::Bool);
    assert_eq!(DT::from_str("Int8").unwrap(), DT::I8);
    assert_eq!(DT::from_str("Int16").unwrap(), DT::I16);
    assert_eq!(DT::from_str("Int32").unwrap(), DT::I32);
    assert_eq!(DT::from_str("Int64").unwrap(), DT::I64);
    assert_eq!(DT::from_str("UInt8").unwrap(), DT::U8);
    assert_eq!(DT::from_str("UInt16").unwrap(), DT::U16);
    assert_eq!(DT::from_str("UInt32").unwrap(), DT::U32);
    assert_eq!(DT::from_str("UInt64").unwrap(), DT::U64);
    assert_eq!(DT::from_str("Float16").unwrap(), DT::F16);
    assert_eq!(DT::from_str("Float32").unwrap(), DT::F32);
    assert_eq!(DT::from_str("Float64").unwrap(), DT::F64);
    assert_eq!(
        DT::from_str("Decimal128(8,-2)").unwrap(),
        DT::Decimal128(8, -2)
    );
    assert_eq!(
        DT::from_str("Decimal128( 8 , -2 )").unwrap(),
        DT::Decimal128(8, -2)
    );
}

macro_rules! test_data_type {
    ($($variant:ident,)*) => {
        mod test_data_type {
            $(
                #[allow(non_snake_case)]
                #[test]
                fn $variant() {
                    let ty = super::super::GenericDataType::$variant;

                    let s = serde_json::to_string(&ty).unwrap();
                    assert_eq!(s, concat!("\"", stringify!($variant), "\""));

                    let rt = serde_json::from_str(&s).unwrap();
                    assert_eq!(ty, rt);
                }
            )*
        }
    };
}

test_data_type!(
    Null, Bool, I8, I16, I32, I64, U8, U16, U32, U64, F16, F32, F64, Utf8, LargeUtf8, List,
    LargeList, Struct, Dictionary, Union, Map, Date64,
);

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

    assert_eq!(schema.fields[0].strategy, Some(Strategy::UtcStrAsDate64));
    assert_eq!(
        schema.fields[0].metadata,
        hash_map!("foo" => "bar", "hello" => "world")
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

    assert_eq!(schema.fields[0].strategy, Some(Strategy::UtcStrAsDate64));
    assert_eq!(
        schema.fields[0].metadata,
        hash_map!("foo" => "bar", "hello" => "world")
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

    assert_error(&res, "Duplicate strategy");
}
