use serde::Deserialize;

use crate::internal::{
    arrow::PrimitiveArrayView, deserialization::integer_deserializer::IntegerDeserializer,
    utils::Mut,
};

use super::outer_sequence_deserializer::OuterSequenceDeserializer;

#[test]
fn example() {
    let mut deser = OuterSequenceDeserializer::new(
        vec![
            (
                String::from("a"),
                IntegerDeserializer::new(PrimitiveArrayView {
                    values: &[1, 2, 3],
                    validity: None,
                })
                .into(),
            ),
            (
                String::from("b"),
                IntegerDeserializer::new(PrimitiveArrayView {
                    values: &[4, 5, 6],
                    validity: None,
                })
                .into(),
            ),
        ],
        3,
    );

    #[derive(Debug, PartialEq, Deserialize)]
    struct Record {
        a: i32,
        b: i32,
    }

    let actual = Vec::<Record>::deserialize(Mut(&mut deser)).unwrap();
    let expected = vec![
        Record { a: 1, b: 4 },
        Record { a: 2, b: 5 },
        Record { a: 3, b: 6 },
    ];

    assert_eq!(actual, expected);
}
