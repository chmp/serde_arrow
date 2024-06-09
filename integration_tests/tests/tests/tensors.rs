use arrow::datatypes::FieldRef;
use serde_arrow::{
    schema::{ext::FixedShapeTensorField, SchemaLike},
    utils::Item,
};
use serde_json::json;

use super::utils::{execute_python, write_file, Result};

/// Test that a fixed shape tensor field can be correctly read in PyArrow
#[test]
fn example() -> Result<()> {
    let items = vec![
        Item(vec![1_i64, 2, 3, 4, 5, 6]),
        Item(vec![7, 8, 9, 0, 1, 2]),
        Item(vec![3, 4, 5, 6, 7, 8]),
        Item(vec![9, 0, 1, 2, 3, 4]),
    ];
    let fields = Vec::<FieldRef>::from_value(&[FixedShapeTensorField::new(
        "item",
        json!({"name": "element", "data_type": "I64"}),
        vec![3, 2, 1],
    )?])?;

    let batch = serde_arrow::to_record_batch(&fields, &items)?;

    write_file("example.ipc", &batch)?;

    let output = execute_python(
        r#"
        import pyarrow as pa
        tbl = tbl = pa.ipc.open_file("example.ipc").read_all()
        print(tbl["item"].combine_chunks().to_numpy_ndarray().shape)
    "#,
    )?;
    assert_eq!(output.trim(), "(4, 3, 2, 1)");
    Ok(())
}
