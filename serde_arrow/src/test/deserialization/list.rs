use marrow::{
    datatypes::FieldMeta,
    view::{ListView, PrimitiveView, View},
};

use crate::internal::{
    deserialization::array_deserializer::ArrayDeserializer, testing::assert_error_contains,
};

#[test]
fn invalid_offsets() {
    let reference = ListView {
        validity: None,
        offsets: &[],
        meta: FieldMeta {
            name: String::from("element"),
            nullable: false,
            metadata: Default::default(),
        },
        elements: Box::new(View::Int32(PrimitiveView {
            validity: None,
            values: &[0, 1, 2, 3, 4, 5],
        })),
    };

    let view = View::List(ListView {
        offsets: &[],
        ..reference.clone()
    });
    assert_error_contains(
        &ArrayDeserializer::new(String::from("foo"), None, view),
        "non empty",
    );

    let view = View::List(ListView {
        offsets: &[0, 5, 2],
        ..reference.clone()
    });
    assert_error_contains(
        &ArrayDeserializer::new(String::from("foo"), None, view),
        "monotonically increasing",
    );
}
