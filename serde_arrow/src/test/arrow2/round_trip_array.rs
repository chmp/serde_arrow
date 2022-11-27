//! Test round trips on the individual array level without the out records
//!

use arrow2::datatypes::{DataType, Field};
use serde::{Deserialize, Serialize};

use crate::{
    arrow2::{sinks::build_dynamic_array_builder, sources::build_dynamic_source},
    base::{deserialize_from_source, serialize_into_sink, Event},
    generic::{
        schema::{FieldBuilder, Tracer},
        sinks::ArrayBuilder,
    },
    test::utils::collect_events,
};

#[test]
fn example() {
    let items: &[i8] = &[0, 1, 2];

    let field = Field::new("value", DataType::Int8, true);
    let mut sink = build_dynamic_array_builder(&field).unwrap();

    for item in items {
        serialize_into_sink(&mut sink, &item).unwrap();
    }

    let array = sink.into_array().unwrap();

    let source = build_dynamic_source(&field, array.as_ref()).unwrap();
    let events = collect_events(source).unwrap();

    // add the outer sequence
    let events = {
        let mut events = events;
        events.insert(0, Event::StartSequence);
        events.push(Event::EndSequence);
        events
    };

    let res: Vec<i8> = deserialize_from_source(&events).unwrap();

    assert_eq!(res, items);
}

macro_rules! test_round_trip {
    (test_name = $test_name:ident, data_type = $data_type:expr, is_nullable = $is_nullable:expr, ty = $ty:ty, values = $values:expr, ) => {
        #[test]
        fn $test_name() {
            let items: &[$ty] = &$values;

            let field = Field::new("value", $data_type, $is_nullable);
            let mut sink = build_dynamic_array_builder(&field).unwrap();

            let mut tracer = Tracer::new();

            for item in items {
                serialize_into_sink(&mut sink, &item).unwrap();
                serialize_into_sink(&mut tracer, &item).unwrap();
            }

            let res_field = tracer.to_field("value").unwrap();
            assert_eq!(res_field, field);

            let array = sink.into_array().unwrap();

            let source = build_dynamic_source(&field, array.as_ref()).unwrap();
            let events = collect_events(source).unwrap();

            // add the outer sequence
            let events = {
                let mut events = events;
                events.insert(0, Event::StartSequence);
                events.push(Event::EndSequence);
                events
            };

            let res_items: Vec<$ty> = deserialize_from_source(&events).unwrap();
            assert_eq!(res_items, items);
        }
    };
}

test_round_trip!(
    test_name = primitive_i8,
    data_type = DataType::Int8,
    is_nullable = false,
    ty = i8,
    values = [0, 1, 2],
);
test_round_trip!(
    test_name = nullable_i8,
    data_type = DataType::Int8,
    is_nullable = true,
    ty = Option<i8>,
    values = [Some(0), None, Some(2)],
);
test_round_trip!(
    test_name = nullable_i8_only_some,
    data_type = DataType::Int8,
    is_nullable = true,
    ty = Option<i8>,
    values = [Some(0), Some(2)],
);

test_round_trip!(
    test_name = primitive_f32,
    data_type = DataType::Float32,
    is_nullable = false,
    ty = f32,
    values = [0.0, 1.0, 2.0],
);
test_round_trip!(
    test_name = nullable_f32,
    data_type = DataType::Float32,
    is_nullable = true,
    ty = Option<f32>,
    values = [Some(0.0), None, Some(2.0)],
);
test_round_trip!(
    test_name = nullable_f32_only_some,
    data_type = DataType::Float32,
    is_nullable = true,
    ty = Option<f32>,
    values = [Some(0.0), Some(2.0)],
);

test_round_trip!(
    test_name = primitive_bool,
    data_type = DataType::Boolean,
    is_nullable = false,
    ty = bool,
    values = [true, false, true],
);
test_round_trip!(
    test_name = nullable_bool,
    data_type = DataType::Boolean,
    is_nullable = true,
    ty = Option<bool>,
    values = [Some(true), None, Some(false)],
);
test_round_trip!(
    test_name = nullable_bool_only_some,
    data_type = DataType::Boolean,
    is_nullable = true,
    ty = Option<bool>,
    values = [Some(true), Some(false)],
);

test_round_trip!(
    test_name = vec_bool,
    data_type = DataType::LargeList(Box::new(Field::new("element", DataType::Boolean, false))),
    is_nullable = false,
    ty = Vec<bool>,
    values = [vec![true, false], vec![], vec![false]],
);
test_round_trip!(
    test_name = nullable_vec_bool,
    data_type = DataType::LargeList(Box::new(Field::new("element", DataType::Boolean, false))),
    is_nullable = true,
    ty = Option<Vec<bool>>,
    values = [Some(vec![true, false]), Some(vec![]), None],
);
test_round_trip!(
    test_name = vec_nullable_bool,
    data_type = DataType::LargeList(Box::new(Field::new("element", DataType::Boolean, true))),
    is_nullable = false,
    ty = Vec<Option<bool>>,
    values = [vec![Some(true), Some(false)], vec![], vec![None, Some(false)]],
);

test_round_trip!(
    test_name = struct_nullable,
    data_type = DataType::Struct(vec![
        Field::new("a", DataType::Boolean, false),
        Field::new("b", DataType::Int64, false),
    ]),
    is_nullable = true,
    ty = Option<BooleanInt64>,
    values = [
        Some(BooleanInt64 {
            a: true,
            b: 42,
        }),
        None,
    ],
);

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
struct BooleanInt64 {
    a: bool,
    b: i64,
}
