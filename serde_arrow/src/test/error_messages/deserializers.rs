use std::collections::HashMap;

use marrow::{
    datatypes::FieldMeta,
    view::{BitsWithOffset, BooleanView, StructView, View},
};
use serde::Deserialize;
use serde_json::json;

use crate::{
    internal::testing::assert_error_contains,
    schema::{SchemaLike, SerdeArrowSchema},
    Deserializer,
};

#[test]
fn example_exhausted() {
    let views = vec![View::Struct(StructView {
        len: 5,
        validity: None,
        fields: vec![(
            FieldMeta {
                name: String::from("nested"),
                nullable: false,
                metadata: HashMap::new(),
            },
            View::Boolean(BooleanView {
                len: 2,
                validity: None,
                values: BitsWithOffset {
                    data: &[0b_0001_0011],
                    offset: 0,
                },
            }),
        )],
    })];

    let schema = SerdeArrowSchema::from_value(json!([{
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
    assert_error_contains(&res, "Out of bounds access");
    assert_error_contains(&res, "field: \"$.item.nested\"");
    assert_error_contains(&res, "data_type: \"Boolean\"");
}
