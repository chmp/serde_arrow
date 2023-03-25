//! Test the schema tracing on the event level
use std::collections::BTreeMap;

use arrow2::datatypes::{DataType, Field};

use crate::{
    internal::{
        event::Event,
        schema::{FieldBuilder, Tracer},
        sink::EventSink,
    },
    schema::{Strategy, STRATEGY_KEY},
};

macro_rules! define_primitive_tests {
    ($event_variant:ident, $data_type:ident) => {
        #[allow(non_snake_case)]
        mod $event_variant {
            use arrow2::datatypes::{DataType, Field};

            use crate::internal::{
                event::Event,
                schema::{FieldBuilder, Tracer},
                sink::EventSink,
            };

            #[test]
            fn single_event() {
                let mut tracer = Tracer::new(Default::default());

                tracer
                    .accept(Event::$event_variant(Default::default()))
                    .unwrap();
                tracer.finish().unwrap();

                let field: Field = tracer.to_field("item").unwrap();
                let expected = Field::new("item", DataType::$data_type, false);

                assert_eq!(field, expected);
            }

            #[test]
            fn multiple_events() {
                let mut tracer = Tracer::new(Default::default());

                tracer
                    .accept(Event::$event_variant(Default::default()))
                    .unwrap();
                tracer
                    .accept(Event::$event_variant(Default::default()))
                    .unwrap();
                tracer.finish().unwrap();

                let field: Field = tracer.to_field("item").unwrap();
                let expected = Field::new("item", DataType::$data_type, false);

                assert_eq!(field, expected);
            }

            #[test]
            fn leading_none() {
                let mut tracer = Tracer::new(Default::default());

                tracer.accept(Event::Null).unwrap();
                tracer
                    .accept(Event::$event_variant(Default::default()))
                    .unwrap();
                tracer.finish().unwrap();

                let field: Field = tracer.to_field("item").unwrap();
                let expected = Field::new("item", DataType::$data_type, true);

                assert_eq!(field, expected);
            }

            #[test]
            fn trailing_none() {
                let mut tracer = Tracer::new(Default::default());

                tracer
                    .accept(Event::$event_variant(Default::default()))
                    .unwrap();
                tracer.accept(Event::Null).unwrap();
                tracer.finish().unwrap();

                let field: Field = tracer.to_field("item").unwrap();
                let expected = Field::new("item", DataType::$data_type, true);

                assert_eq!(field, expected);
            }

            #[test]
            fn leading_some() {
                let mut tracer = Tracer::new(Default::default());

                tracer.accept(Event::Some).unwrap();
                tracer
                    .accept(Event::$event_variant(Default::default()))
                    .unwrap();
                tracer
                    .accept(Event::$event_variant(Default::default()))
                    .unwrap();
                tracer.finish().unwrap();

                let field: Field = tracer.to_field("item").unwrap();
                let expected = Field::new("item", DataType::$data_type, true);

                assert_eq!(field, expected);
            }

            #[test]
            fn trailing_some() {
                let mut tracer = Tracer::new(Default::default());

                tracer
                    .accept(Event::$event_variant(Default::default()))
                    .unwrap();
                tracer.accept(Event::Some).unwrap();
                tracer
                    .accept(Event::$event_variant(Default::default()))
                    .unwrap();
                tracer.finish().unwrap();

                let field: Field = tracer.to_field("item").unwrap();
                let expected = Field::new("item", DataType::$data_type, true);

                assert_eq!(field, expected);
            }
        }
    };
}

define_primitive_tests!(Bool, Boolean);
define_primitive_tests!(Str, LargeUtf8);
define_primitive_tests!(OwnedStr, LargeUtf8);
define_primitive_tests!(I8, Int8);
define_primitive_tests!(I16, Int16);
define_primitive_tests!(I32, Int32);
define_primitive_tests!(I64, Int64);
define_primitive_tests!(U8, UInt8);
define_primitive_tests!(U16, UInt16);
define_primitive_tests!(U32, UInt32);
define_primitive_tests!(U64, UInt64);
define_primitive_tests!(F32, Float32);
define_primitive_tests!(F64, Float64);

#[test]
fn empty_list() {
    let mut tracer = Tracer::new(Default::default());
    tracer.accept(Event::StartSequence).unwrap();
    tracer.accept(Event::EndSequence).unwrap();
    tracer.finish().unwrap();

    let field: Field = tracer.to_field("root").unwrap();
    let expected = Field::new(
        "root",
        DataType::LargeList(Box::new(Field::new("element", DataType::Null, false))),
        false,
    );

    assert_eq!(field, expected);
}

#[test]
fn incomplete_list() {
    let mut tracer = Tracer::new(Default::default());
    tracer.accept(Event::StartSequence).unwrap();
    assert!(tracer.finish().is_err());
}

#[test]
fn nullable_list_null_first() {
    let mut tracer = Tracer::new(Default::default());
    tracer.accept(Event::Null).unwrap();
    tracer.accept(Event::StartSequence).unwrap();
    tracer.accept(Event::EndSequence).unwrap();
    tracer.finish().unwrap();

    let field: Field = tracer.to_field("root").unwrap();
    let expected = Field::new(
        "root",
        DataType::LargeList(Box::new(Field::new("element", DataType::Null, false))),
        true,
    );

    assert_eq!(field, expected);
}

#[test]
fn nullable_list_null_second() {
    let mut tracer = Tracer::new(Default::default());
    tracer.accept(Event::StartSequence).unwrap();
    tracer.accept(Event::EndSequence).unwrap();
    tracer.accept(Event::Null).unwrap();
    tracer.finish().unwrap();

    let field: Field = tracer.to_field("root").unwrap();
    let expected = Field::new(
        "root",
        DataType::LargeList(Box::new(Field::new("element", DataType::Null, false))),
        true,
    );

    assert_eq!(field, expected);
}

#[test]
fn nullable_list_some_first() {
    let mut tracer = Tracer::new(Default::default());
    tracer.accept(Event::Some).unwrap();
    tracer.accept(Event::StartSequence).unwrap();
    tracer.accept(Event::EndSequence).unwrap();
    tracer.accept(Event::StartSequence).unwrap();
    tracer.accept(Event::EndSequence).unwrap();
    tracer.finish().unwrap();

    let field: Field = tracer.to_field("root").unwrap();
    let expected = Field::new(
        "root",
        DataType::LargeList(Box::new(Field::new("element", DataType::Null, false))),
        true,
    );

    assert_eq!(field, expected);
}

#[test]
fn nullable_list_some_second() {
    let mut tracer = Tracer::new(Default::default());
    tracer.accept(Event::StartSequence).unwrap();
    tracer.accept(Event::EndSequence).unwrap();
    tracer.accept(Event::Some).unwrap();
    tracer.accept(Event::StartSequence).unwrap();
    tracer.accept(Event::EndSequence).unwrap();
    tracer.finish().unwrap();

    let field: Field = tracer.to_field("root").unwrap();
    let expected = Field::new(
        "root",
        DataType::LargeList(Box::new(Field::new("element", DataType::Null, false))),
        true,
    );

    assert_eq!(field, expected);
}

#[test]
fn primitive_lists() {
    let mut tracer = Tracer::new(Default::default());
    tracer.accept(Event::StartSequence).unwrap();
    tracer.accept(Event::I8(0)).unwrap();
    tracer.accept(Event::I8(1)).unwrap();
    tracer.accept(Event::EndSequence).unwrap();
    tracer.accept(Event::StartSequence).unwrap();
    tracer.accept(Event::I8(2)).unwrap();
    tracer.accept(Event::EndSequence).unwrap();
    tracer.accept(Event::StartSequence).unwrap();
    tracer.accept(Event::EndSequence).unwrap();
    tracer.finish().unwrap();

    let field: Field = tracer.to_field("root").unwrap();
    let expected = Field::new(
        "root",
        DataType::LargeList(Box::new(Field::new("element", DataType::Int8, false))),
        false,
    );

    assert_eq!(field, expected);
}

#[test]
fn nested_lists() {
    let mut tracer = Tracer::new(Default::default());
    tracer.accept(Event::StartSequence).unwrap();
    tracer.accept(Event::StartSequence).unwrap();
    tracer.accept(Event::I8(0)).unwrap();
    tracer.accept(Event::I8(1)).unwrap();
    tracer.accept(Event::EndSequence).unwrap();
    tracer.accept(Event::EndSequence).unwrap();
    tracer.accept(Event::StartSequence).unwrap();
    tracer.accept(Event::StartSequence).unwrap();
    tracer.accept(Event::I8(2)).unwrap();
    tracer.accept(Event::EndSequence).unwrap();
    tracer.accept(Event::StartSequence).unwrap();
    tracer.accept(Event::I8(3)).unwrap();
    tracer.accept(Event::EndSequence).unwrap();
    tracer.accept(Event::EndSequence).unwrap();
    tracer.accept(Event::StartSequence).unwrap();
    tracer.accept(Event::EndSequence).unwrap();
    tracer.finish().unwrap();

    let field: Field = tracer.to_field("root").unwrap();
    let expected = Field::new(
        "root",
        DataType::LargeList(Box::new(Field::new(
            "element",
            DataType::LargeList(Box::new(Field::new("element", DataType::Int8, false))),
            false,
        ))),
        false,
    );

    assert_eq!(field, expected);
}

#[test]
fn structs() {
    let mut tracer = Tracer::new(Default::default());
    tracer.accept(Event::StartStruct).unwrap();
    tracer.accept(Event::Str("hello")).unwrap();
    tracer.accept(Event::U8(0)).unwrap();
    tracer.accept(Event::Str("world")).unwrap();
    tracer.accept(Event::I32(1)).unwrap();
    tracer.accept(Event::EndStruct).unwrap();
    tracer.accept(Event::StartStruct).unwrap();
    tracer.accept(Event::Str("hello")).unwrap();
    tracer.accept(Event::U8(0)).unwrap();
    tracer.accept(Event::Str("world")).unwrap();
    tracer.accept(Event::I32(1)).unwrap();
    tracer.accept(Event::EndStruct).unwrap();
    tracer.finish().unwrap();

    let field: Field = tracer.to_field("root").unwrap();
    let expected = Field::new(
        "root",
        DataType::Struct(vec![
            Field::new("hello", DataType::UInt8, false),
            Field::new("world", DataType::Int32, false),
        ]),
        false,
    );

    assert_eq!(field, expected);
}

#[test]
fn struct_nullable_trailing_null() {
    let mut tracer = Tracer::new(Default::default());
    tracer.accept(Event::StartStruct).unwrap();
    tracer.accept(Event::Str("hello")).unwrap();
    tracer.accept(Event::U8(0)).unwrap();
    tracer.accept(Event::Str("world")).unwrap();
    tracer.accept(Event::I32(1)).unwrap();
    tracer.accept(Event::EndStruct).unwrap();
    tracer.accept(Event::Null).unwrap();
    tracer.finish().unwrap();

    let field: Field = tracer.to_field("root").unwrap();
    let expected = Field::new(
        "root",
        DataType::Struct(vec![
            Field::new("hello", DataType::UInt8, false),
            Field::new("world", DataType::Int32, false),
        ]),
        true,
    );

    assert_eq!(field, expected);
}

#[test]
fn struct_nullable_leading_null() {
    let mut tracer = Tracer::new(Default::default());
    tracer.accept(Event::Null).unwrap();
    tracer.accept(Event::StartStruct).unwrap();
    tracer.accept(Event::Str("hello")).unwrap();
    tracer.accept(Event::U8(0)).unwrap();
    tracer.accept(Event::Str("world")).unwrap();
    tracer.accept(Event::I32(1)).unwrap();
    tracer.accept(Event::EndStruct).unwrap();
    tracer.finish().unwrap();

    let field: Field = tracer.to_field("root").unwrap();
    let expected = Field::new(
        "root",
        DataType::Struct(vec![
            Field::new("hello", DataType::UInt8, false),
            Field::new("world", DataType::Int32, false),
        ]),
        true,
    );

    assert_eq!(field, expected);
}

#[test]
fn struct_nested_structs() {
    let mut tracer = Tracer::new(Default::default());
    tracer.accept(Event::StartStruct).unwrap();
    tracer.accept(Event::Str("hello")).unwrap();
    tracer.accept(Event::U8(0)).unwrap();
    tracer.accept(Event::Str("world")).unwrap();
    tracer.accept(Event::StartStruct).unwrap();
    tracer.accept(Event::Str("foo")).unwrap();
    tracer.accept(Event::I32(1)).unwrap();
    tracer.accept(Event::EndStruct).unwrap();
    tracer.accept(Event::EndStruct).unwrap();
    tracer.finish().unwrap();

    let field: Field = tracer.to_field("root").unwrap();
    let expected = Field::new(
        "root",
        DataType::Struct(vec![
            Field::new("hello", DataType::UInt8, false),
            Field::new(
                "world",
                DataType::Struct(vec![Field::new("foo", DataType::Int32, false)]),
                false,
            ),
        ]),
        false,
    );

    assert_eq!(field, expected);
}

#[test]
fn struct_nested_list() {
    let mut tracer = Tracer::new(Default::default());
    tracer.accept(Event::StartStruct).unwrap();
    tracer.accept(Event::Str("hello")).unwrap();
    tracer.accept(Event::U8(0)).unwrap();
    tracer.accept(Event::Str("world")).unwrap();
    tracer.accept(Event::StartSequence).unwrap();
    tracer.accept(Event::Str("foo")).unwrap();
    tracer.accept(Event::Str("bar")).unwrap();
    tracer.accept(Event::Str("baz")).unwrap();
    tracer.accept(Event::EndSequence).unwrap();
    tracer.accept(Event::EndStruct).unwrap();
    tracer.finish().unwrap();

    let field: Field = tracer.to_field("root").unwrap();
    let expected = Field::new(
        "root",
        DataType::Struct(vec![
            Field::new("hello", DataType::UInt8, false),
            Field::new(
                "world",
                DataType::LargeList(Box::new(Field::new("element", DataType::LargeUtf8, false))),
                false,
            ),
        ]),
        false,
    );

    assert_eq!(field, expected);
}

#[test]
fn struct_tuple() {
    let mut tracer = Tracer::new(Default::default());
    tracer.accept(Event::StartTuple).unwrap();
    tracer.accept(Event::U8(0)).unwrap();
    tracer.accept(Event::U8(1)).unwrap();
    tracer.accept(Event::U8(2)).unwrap();
    tracer.accept(Event::EndTuple).unwrap();
    tracer.finish().unwrap();

    let field: Field = tracer.to_field("root").unwrap();
    let mut expected = Field::new(
        "root",
        DataType::Struct(vec![
            Field::new("0", DataType::UInt8, false),
            Field::new("1", DataType::UInt8, false),
            Field::new("2", DataType::UInt8, false),
        ]),
        false,
    );
    expected.metadata = Strategy::TupleAsStruct.into();

    assert_eq!(field, expected);
}

#[test]
fn struct_tuple_struct_nested() {
    let mut tracer = Tracer::new(Default::default());
    tracer.accept(Event::StartTuple).unwrap();
    tracer.accept(Event::StartStruct).unwrap();
    tracer.accept(Event::Str("foo")).unwrap();
    tracer.accept(Event::U8(0)).unwrap();
    tracer.accept(Event::Str("bar")).unwrap();
    tracer.accept(Event::StartTuple).unwrap();
    tracer.accept(Event::U8(1)).unwrap();
    tracer.accept(Event::U8(2)).unwrap();
    tracer.accept(Event::EndTuple).unwrap();
    tracer.accept(Event::EndStruct).unwrap();
    tracer.accept(Event::U8(3)).unwrap();
    tracer.accept(Event::EndTuple).unwrap();
    tracer.finish().unwrap();

    let mut metadata: BTreeMap<String, String> = Default::default();
    metadata.insert(
        STRATEGY_KEY.to_string(),
        Strategy::TupleAsStruct.to_string(),
    );

    let field: Field = tracer.to_field("root").unwrap();
    let expected = Field::new(
        "root",
        DataType::Struct(vec![
            Field::new(
                "0",
                DataType::Struct(vec![
                    Field::new("foo", DataType::UInt8, false),
                    Field::new(
                        "bar",
                        DataType::Struct(vec![
                            Field::new("0", DataType::UInt8, false),
                            Field::new("1", DataType::UInt8, false),
                        ]),
                        false,
                    )
                    .with_metadata(metadata.clone()),
                ]),
                false,
            ),
            Field::new("1", DataType::UInt8, false),
        ]),
        false,
    )
    .with_metadata(metadata);

    assert_eq!(field, expected);
}
