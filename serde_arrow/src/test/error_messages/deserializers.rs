use std::collections::HashMap;

use serde::Deserialize;
use serde_json::json;

use crate::{
    internal::{
        arrow::{ArrayView, BitsWithOffset, BooleanArrayView, FieldMeta, StructArrayView},
        testing::assert_error_contains,
    },
    schema::{SchemaLike, SerdeArrowSchema},
    Deserializer,
};

#[test]
fn example_exhausted() {
    let views = vec![ArrayView::Struct(StructArrayView {
        len: 5,
        validity: None,
        fields: vec![(
            ArrayView::Boolean(BooleanArrayView {
                len: 2,
                validity: None,
                values: BitsWithOffset {
                    data: &[0b_0001_0011],
                    offset: 0,
                },
            }),
            FieldMeta {
                name: String::from("nested"),
                nullable: false,
                metadata: HashMap::new(),
            },
        )],
    })];

    let schema = SerdeArrowSchema::from_value(&json!([{
        "name": "item",
        "data_type": "Struct",
        "children": [
            {"name": "nested", "data_type": "Bool"},
        ],
    }]))
    .unwrap();

    let deserializer = Deserializer::new(&schema.fields, views).unwrap();

    #[derive(Deserialize)]
    struct S {
        #[allow(dead_code)]
        item: Nested,
    }

    #[derive(Deserialize)]
    struct Nested {
        #[allow(dead_code)]
        nested: bool,
    }

    let res = Vec::<S>::deserialize(deserializer);
    assert_error_contains(&res, "Exhausted deserializer");
    assert_error_contains(&res, "field: \"$.item.nested\"");
    assert_error_contains(&res, "data_type: \"Boolean\"");
}
