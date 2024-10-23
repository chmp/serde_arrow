use crate::internal::{
    arrow::{ArrayView, FieldMeta, ListArrayView, PrimitiveArrayView},
    deserialization::array_deserializer::ArrayDeserializer,
    testing::assert_error_contains,
};

#[test]
fn invalid_offsets() {
    let reference = ListArrayView {
        validity: None,
        offsets: &[],
        meta: FieldMeta {
            name: String::from("element"),
            nullable: false,
            metadata: Default::default(),
        },
        element: Box::new(ArrayView::Int32(PrimitiveArrayView {
            validity: None,
            values: &[0, 1, 2, 3, 4, 5],
        })),
    };

    let view = ArrayView::List(ListArrayView {
        offsets: &[],
        ..reference.clone()
    });
    assert_error_contains(
        &ArrayDeserializer::new(String::from("foo"), None, view),
        "non empty",
    );

    let view = ArrayView::List(ListArrayView {
        offsets: &[0, 5, 2],
        ..reference.clone()
    });
    assert_error_contains(
        &ArrayDeserializer::new(String::from("foo"), None, view),
        "monotonically increasing",
    );
}
