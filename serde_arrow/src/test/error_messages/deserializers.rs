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

    #[derive(Deserialize, Debug)]
    struct S {
        #[allow(dead_code)]
        item: Nested,
    }

    #[derive(Deserialize, Debug)]
    struct Nested {
        #[allow(dead_code)]
        nested: bool,
    }

    let err = Vec::<S>::deserialize(deserializer).unwrap_err();
    assert_error_contains(&err, "Out of bounds access");
    assert_error_contains(&err, "field: \"$.item.nested\"");
    assert_error_contains(&err, "data_type: \"Boolean\"");
}
