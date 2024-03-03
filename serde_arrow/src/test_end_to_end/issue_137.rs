//! Test arrow integration

use std::sync::Arc;

use serde::{Deserialize, Serialize};

use crate::{self as serde_arrow, _impl::{arrow::{datatypes::Field, _raw::{array::RecordBatch, schema::Schema}}, PanicOnError}, schema::{SchemaLike, TracingOptions}};

#[test]
fn example() -> PanicOnError<()> {
    #[derive(Debug, PartialEq, Serialize, Deserialize)]
    struct Record {
        a: i32,
        b: String,
    }

    let items_input = vec![
        Record { a: 21, b: String::from("first") },
        Record { a: 42, b: String::from("second") },
    ];

    let fields_from_type = Vec::<Field>::from_type::<Record>(TracingOptions::default())?;
    let arrays = serde_arrow::to_arrow(&fields_from_type, &items_input)?;

    let batch = RecordBatch::try_new(Arc::new(Schema::new(fields_from_type.clone())), arrays)?;

    let fields = Vec::<Field>::from_value(&batch.schema())?;
    let items: Vec<Record> = serde_arrow::from_arrow(&fields, batch.columns())?;

    assert_eq!(fields, fields_from_type);
    assert_eq!(items, items_input);

    Ok(())
}