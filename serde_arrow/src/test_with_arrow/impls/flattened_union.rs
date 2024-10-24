use std::collections::HashMap;

use crate::{
    internal::{
        array_builder::ArrayBuilder,
        arrow::{Array, DataType, Field},
        schema::{SchemaLike, TracingOptions},
    },
    schema::SerdeArrowSchema,
    Serializer,
};
use serde::{Deserialize, Serialize};

#[derive(Serialize, Deserialize)]
struct Number {
    v: Value,
}

#[derive(Serialize, Deserialize)]
enum Value {
    Real { value: f32 },
    Complex { i: f32, j: f32 },
    Whole { value: usize },
}

fn number_field() -> Field {
    Field {
        name: "v".to_string(),
        data_type: DataType::Struct(vec![
            Field {
                name: "Complex::i".to_string(),
                data_type: DataType::Float32,
                nullable: true,
                metadata: HashMap::new(),
            },
            Field {
                name: "Complex::j".to_string(),
                data_type: DataType::Float32,
                nullable: true,
                metadata: HashMap::new(),
            },
            Field {
                name: "Real::value".to_string(),
                data_type: DataType::Float32,
                nullable: true,
                metadata: HashMap::new(),
            },
            Field {
                name: "Whole::value".to_string(),
                data_type: DataType::UInt64,
                nullable: true,
                metadata: HashMap::new(),
            },
        ]),
        nullable: false,
        metadata: HashMap::from([(
            "SERDE_ARROW:strategy".to_string(),
            "EnumsWithNamedFieldsAsStructs".to_string(),
        )]),
    }
}

fn number_schema() -> SerdeArrowSchema {
    let options = TracingOptions::default()
        .allow_null_fields(true)
        .enums_with_named_fields_as_structs(true);

    SerdeArrowSchema::from_type::<Number>(options).unwrap()
}

fn number_data() -> Vec<Number> {
    vec![
        Number {
            v: Value::Real { value: 0.0 },
        },
        Number {
            v: Value::Complex { i: 0.5, j: 0.5 },
        },
        Number {
            v: Value::Whole { value: 5 },
        },
    ]
}

#[test]
fn test_build_flattened_union_builder() {
    let mut builder = ArrayBuilder::new(number_schema()).unwrap();

    // One struct in the array
    let arrays = builder.build_arrays().unwrap();

    assert_eq!(arrays.len(), 1);

    let array = &arrays[0];

    let Array::Struct(ref struct_array) = array else {
        panic!("expected a struct array, found {array:#?}");
    };

    // Should be a single struct array with 4 fields: Complex::i, Complex::j, Real::value, Whole::value
    assert_eq!(
        struct_array.fields.len(),
        4,
        "contained {} fields",
        struct_array.fields.len()
    );

    let (first_field, meta) = &struct_array.fields[0];
    assert_eq!(meta.name, "Complex::i");
    assert!(matches!(first_field, Array::Float32(_)));

    let (second_field, meta) = &struct_array.fields[1];
    assert_eq!(meta.name, "Complex::j");
    assert!(matches!(second_field, Array::Float32(_)));

    let (third_field, meta) = &struct_array.fields[2];
    assert_eq!(meta.name, "Real::value");
    assert!(matches!(third_field, Array::Float32(_)));

    let (fourth_field, meta) = &struct_array.fields[3];
    assert_eq!(meta.name, "Whole::value");
    assert!(matches!(fourth_field, Array::UInt64(_)));
}

#[test]
fn test_serialize_flattened_union_builder() {
    let field = number_field();
    let data = number_data();
    let schema = SerdeArrowSchema {
        fields: vec![field],
    };

    let api_builder = ArrayBuilder::new(schema).expect("failed to create api array builder");
    let serializer = Serializer::new(api_builder);
    data.serialize(serializer)
        .expect("failed to serialize")
        .into_inner()
        .to_arrow()
        .expect("failed to serialize to arrow");
}

#[test]
fn test_record_batch_flattened_union_builder() {
    let field = number_field();
    let data = number_data();
    let schema = SerdeArrowSchema {
        fields: vec![field],
    };

    let api_builder = ArrayBuilder::new(schema).expect("failed to create api array builder");
    let serializer = Serializer::new(api_builder);
    data.serialize(serializer)
        .expect("failed to serialize")
        .into_inner()
        .to_record_batch()
        .expect("failed to create record batch");
}

#[derive(Serialize, Deserialize)]
struct ComplexMessage {
    data: MsgData,
}

#[derive(Serialize, Deserialize)]
enum MsgData {
    One { data: usize },
    Two { opts: MsgOptions },
}

#[derive(Serialize, Deserialize)]
struct MsgOptions {
    loc: Location,
}

#[derive(Serialize, Deserialize)]
enum Location {
    Left,
    Right,
}

fn nested_enum_schema() -> SerdeArrowSchema {
    let options = TracingOptions::default()
        .allow_null_fields(true)
        .enums_without_data_as_strings(true)
        .enums_with_named_fields_as_structs(true);

    SerdeArrowSchema::from_type::<ComplexMessage>(options).unwrap()
}

fn nested_enum_data() -> Vec<ComplexMessage> {
    vec![
        ComplexMessage {
            data: MsgData::One { data: 3 },
        },
        ComplexMessage {
            data: MsgData::Two {
                opts: MsgOptions {
                    loc: Location::Right,
                },
            },
        },
    ]
}

#[test]
fn test_flattened_union_with_nested_enum() {
    let mut builder = ArrayBuilder::new(nested_enum_schema()).unwrap();

    // One struct in the array
    let arrays = builder.build_arrays().unwrap();

    println!("{arrays:#?}");

    assert_eq!(arrays.len(), 1);

    let array = &arrays[0];

    let Array::Struct(ref struct_array) = array else {
        panic!("expected a struct array, found {array:#?}");
    };

    let serializer = Serializer::new(builder);
    nested_enum_data()
        .serialize(serializer)
        .expect("failed to serialize")
        .into_inner()
        .to_arrow()
        .expect("failed to serialize to arrow");
}
