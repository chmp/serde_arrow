use crate::internal::{
    arrow::{ArrayView, DenseUnionArrayView, FieldMeta, PrimitiveArrayView},
    deserialization::array_deserializer::ArrayDeserializer,
    testing::assert_error_contains,
};

#[test]
fn non_consecutive_offsets() {
    let fields = vec![
        (
            0,
            ArrayView::Int32(PrimitiveArrayView {
                validity: None,
                values: &[1, 2, 3, 4, 5, 6],
            }),
            FieldMeta {
                name: String::from("foo"),
                nullable: false,
                metadata: Default::default(),
            },
        ),
        (
            1,
            ArrayView::Int32(PrimitiveArrayView {
                validity: None,
                values: &[1, 2, 3, 4, 5, 6],
            }),
            FieldMeta {
                name: String::from("foo"),
                nullable: false,
                metadata: Default::default(),
            },
        ),
    ];

    // first type has an unused element
    let view = ArrayView::DenseUnion(DenseUnionArrayView {
        types: &[0, 0, 1],
        offsets: &[0, 2, 0],
        fields: fields.clone(),
    });
    assert_error_contains(
        &ArrayDeserializer::new(String::from("foo"), None, view),
        "consecutive offsets",
    );

    // first type has an unused element
    let view = ArrayView::DenseUnion(DenseUnionArrayView {
        types: &[0, 0, 0],
        offsets: &[0, 1, 4],
        fields: fields.clone(),
    });
    assert_error_contains(
        &ArrayDeserializer::new(String::from("foo"), None, view),
        "consecutive offsets",
    );
}
