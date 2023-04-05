//! Test the schema tracing on the event level
use crate::internal::schema::{GenericDataType, GenericField};
use crate::schema::TracingOptions;
use crate::{
    internal::{event::Event, schema::Tracer, sink::EventSink},
    schema::Strategy,
};

macro_rules! define_primitive_tests {
    ($event_variant:ident, $data_type:ident) => {
        #[allow(non_snake_case)]
        mod $event_variant {
            use crate::internal::{
                event::Event,
                schema::{GenericDataType, GenericField, Tracer},
                sink::EventSink,
            };

            #[test]
            fn single_event() {
                let mut tracer = Tracer::new(Default::default());

                tracer
                    .accept(Event::$event_variant(Default::default()))
                    .unwrap();
                tracer.finish().unwrap();

                let field = tracer.to_field("item").unwrap();
                let expected = GenericField::new("item", GenericDataType::$data_type, false);

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

                let field = tracer.to_field("item").unwrap();
                let expected = GenericField::new("item", GenericDataType::$data_type, false);

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

                let field = tracer.to_field("item").unwrap();
                let expected = GenericField::new("item", GenericDataType::$data_type, true);

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

                let field = tracer.to_field("item").unwrap();
                let expected = GenericField::new("item", GenericDataType::$data_type, true);

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

                let field = tracer.to_field("item").unwrap();
                let expected = GenericField::new("item", GenericDataType::$data_type, true);

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

                let field = tracer.to_field("item").unwrap();
                let expected = GenericField::new("item", GenericDataType::$data_type, true);

                assert_eq!(field, expected);
            }
        }
    };
}

define_primitive_tests!(Bool, Bool);
define_primitive_tests!(Str, LargeUtf8);
define_primitive_tests!(OwnedStr, LargeUtf8);
define_primitive_tests!(I8, I8);
define_primitive_tests!(I16, I16);
define_primitive_tests!(I32, I32);
define_primitive_tests!(I64, I64);
define_primitive_tests!(U8, U8);
define_primitive_tests!(U16, U16);
define_primitive_tests!(U32, U32);
define_primitive_tests!(U64, U64);
define_primitive_tests!(F32, F32);
define_primitive_tests!(F64, F64);

#[test]
fn empty_list() {
    // without items the type cannot be determined
    let mut tracer = Tracer::new(TracingOptions::default().allow_null_fields(true));
    tracer.accept(Event::StartSequence).unwrap();
    tracer.accept(Event::EndSequence).unwrap();
    tracer.finish().unwrap();

    let field = tracer.to_field("root").unwrap();
    let expected = GenericField::new("root", GenericDataType::LargeList, false)
        .with_child(GenericField::new("element", GenericDataType::Null, false));

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
    // NOTE: here the type cannot be determined
    let mut tracer = Tracer::new(TracingOptions::default().allow_null_fields(true));
    tracer.accept(Event::Null).unwrap();
    tracer.accept(Event::StartSequence).unwrap();
    tracer.accept(Event::EndSequence).unwrap();
    tracer.finish().unwrap();

    let field = tracer.to_field("root").unwrap();
    let expected = GenericField::new("root", GenericDataType::LargeList, true)
        .with_child(GenericField::new("element", GenericDataType::Null, false));

    assert_eq!(field, expected);
}

#[test]
fn nullable_list_null_second() {
    // NOTE: here the type cannot be determined
    let mut tracer = Tracer::new(TracingOptions::default().allow_null_fields(true));
    tracer.accept(Event::StartSequence).unwrap();
    tracer.accept(Event::EndSequence).unwrap();
    tracer.accept(Event::Null).unwrap();
    tracer.finish().unwrap();

    let field = tracer.to_field("root").unwrap();
    let expected = GenericField::new("root", GenericDataType::LargeList, true)
        .with_child(GenericField::new("element", GenericDataType::Null, false));

    assert_eq!(field, expected);
}

#[test]
fn nullable_list_some_first() {
    // NOTE: here the type cannot be determined
    let mut tracer = Tracer::new(TracingOptions::default().allow_null_fields(true));
    tracer.accept(Event::Some).unwrap();
    tracer.accept(Event::StartSequence).unwrap();
    tracer.accept(Event::EndSequence).unwrap();
    tracer.accept(Event::StartSequence).unwrap();
    tracer.accept(Event::EndSequence).unwrap();
    tracer.finish().unwrap();

    let field = tracer.to_field("root").unwrap();
    let expected = GenericField::new("root", GenericDataType::LargeList, true)
        .with_child(GenericField::new("element", GenericDataType::Null, false));

    assert_eq!(field, expected);
}

#[test]
fn nullable_list_some_second() {
    // NOTE: here the type cannot be determined
    let mut tracer = Tracer::new(TracingOptions::default().allow_null_fields(true));
    tracer.accept(Event::StartSequence).unwrap();
    tracer.accept(Event::EndSequence).unwrap();
    tracer.accept(Event::Some).unwrap();
    tracer.accept(Event::StartSequence).unwrap();
    tracer.accept(Event::EndSequence).unwrap();
    tracer.finish().unwrap();

    let field = tracer.to_field("root").unwrap();
    let expected = GenericField::new("root", GenericDataType::LargeList, true)
        .with_child(GenericField::new("element", GenericDataType::Null, false));

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

    let field = tracer.to_field("root").unwrap();
    let expected = GenericField::new("root", GenericDataType::LargeList, false)
        .with_child(GenericField::new("element", GenericDataType::I8, false));

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

    let field = tracer.to_field("root").unwrap();

    let expected = GenericField::new("root", GenericDataType::LargeList, false).with_child(
        GenericField::new("element", GenericDataType::LargeList, false)
            .with_child(GenericField::new("element", GenericDataType::I8, false)),
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

    let field = tracer.to_field("root").unwrap();

    let mut expected = GenericField::new("root", GenericDataType::Struct, false);
    expected.children = vec![
        GenericField::new("hello", GenericDataType::U8, false),
        GenericField::new("world", GenericDataType::I32, false),
    ];

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

    let field: GenericField = tracer.to_field("root").unwrap();

    let mut expected = GenericField::new("root", GenericDataType::Struct, true);
    expected.children = vec![
        GenericField::new("hello", GenericDataType::U8, false),
        GenericField::new("world", GenericDataType::I32, false),
    ];

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

    let field = tracer.to_field("root").unwrap();

    let mut expected = GenericField::new("root", GenericDataType::Struct, true);
    expected.children = vec![
        GenericField::new("hello", GenericDataType::U8, false),
        GenericField::new("world", GenericDataType::I32, false),
    ];

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

    let field: GenericField = tracer.to_field("root").unwrap();

    let mut expected = GenericField::new("root", GenericDataType::Struct, false);
    expected.children =
        vec![
            GenericField::new("hello", GenericDataType::U8, false),
            GenericField::new("world", GenericDataType::Struct, false)
                .with_child(GenericField::new("foo", GenericDataType::I32, false)),
        ];

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

    let field = tracer.to_field("root").unwrap();

    let mut expected = GenericField::new("root", GenericDataType::Struct, false);
    expected.children = vec![
        GenericField::new("hello", GenericDataType::U8, false),
        GenericField::new("world", GenericDataType::LargeList, false).with_child(
            GenericField::new("element", GenericDataType::LargeUtf8, false),
        ),
    ];

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

    let field: GenericField = tracer.to_field("root").unwrap();

    let mut expected = GenericField::new("root", GenericDataType::Struct, false);
    expected.children = vec![
        GenericField::new("0", GenericDataType::U8, false),
        GenericField::new("1", GenericDataType::U8, false),
        GenericField::new("2", GenericDataType::U8, false),
    ];
    expected.strategy = Some(Strategy::TupleAsStruct);

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

    let field: GenericField = tracer.to_field("root").unwrap();

    let mut expected2 = GenericField::new("bar", GenericDataType::Struct, false);
    expected2.children = vec![
        GenericField::new("0", GenericDataType::U8, false),
        GenericField::new("1", GenericDataType::U8, false),
    ];
    expected2.strategy = Some(Strategy::TupleAsStruct);

    let mut expected1 = GenericField::new("0", GenericDataType::Struct, false);
    expected1.children = vec![
        GenericField::new("foo", GenericDataType::U8, false),
        expected2,
    ];

    let mut expected = GenericField::new("root", GenericDataType::Struct, false);
    expected.children = vec![
        expected1,
        GenericField::new("1", GenericDataType::U8, false),
    ];
    expected.strategy = Some(Strategy::TupleAsStruct);

    assert_eq!(field, expected);
}
