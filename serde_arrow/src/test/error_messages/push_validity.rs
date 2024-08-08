use serde::Serialize;
use serde_json::json;

use crate::internal::{array_builder::ArrayBuilder, error::PanicOnError, schema::{SchemaLike, SerdeArrowSchema}, testing::assert_error};


#[test]
fn push_validity_issue_202() -> PanicOnError<()> {
    let schema = SerdeArrowSchema::from_value(&json!([
        {
            "name": "nested", 
            "data_type": "Struct", 
            "children": [
                {"name": "field", "data_type": "U32"},
            ],
        },
    ]))?;
    
    #[derive(Serialize)]
    struct Record {
        nested: Nested,
    }

    #[derive(Serialize)]
    struct Nested {
        field: Option<u32>,
    }

    let mut array_builder = ArrayBuilder::new(schema)?;
    let res = array_builder.push(&Record { nested: Nested { field: Some(5) }});
    assert_eq!(res, Ok(()));

    let res = array_builder.push(&Record { nested: Nested { field: None }});
    assert_error(&res, "field: \"nested.field\"");
    
    Ok(())
}