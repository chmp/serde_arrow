use marrow::{
    datatypes::FieldMeta,
    view::{PrimitiveView, UnionView, View},
};

use crate::internal::{
    deserialization::array_deserializer::ArrayDeserializer, testing::assert_error_contains,
};

#[test]
fn non_consecutive_offsets() {
    let fields = vec![
        (
            0,
            FieldMeta {
                name: String::from("foo"),
                nullable: false,
                metadata: Default::default(),
            },
            View::Int32(PrimitiveView {
                validity: None,
                values: &[1, 2, 3, 4, 5, 6],
            }),
        ),
        (
            1,
            FieldMeta {
                name: String::from("foo"),
                nullable: false,
                metadata: Default::default(),
            },
            View::Int32(PrimitiveView {
                validity: None,
                values: &[1, 2, 3, 4, 5, 6],
            }),
        ),
    ];

    // first type has an unused element
    let view = View::Union(UnionView {
        types: &[0, 0, 1],
        offsets: Some(&[0, 2, 0]),
        fields: fields.clone(),
    });
    assert_error_contains(
        &ArrayDeserializer::new(String::from("foo"), None, view),
        "consecutive offsets",
    );

    // first type has an unused element
    let view = View::Union(UnionView {
        types: &[0, 0, 0],
        offsets: Some(&[0, 1, 4]),
        fields: fields.clone(),
    });
    assert_error_contains(
        &ArrayDeserializer::new(String::from("foo"), None, view),
        "consecutive offsets",
    );
}
