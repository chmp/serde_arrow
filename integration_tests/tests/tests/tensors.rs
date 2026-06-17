use arrow::datatypes::FieldRef;
use serde::Serialize;
use serde_arrow::{
    schema::{
        ext::{FixedShapeTensorField, VariableShapeTensorField},
        SchemaLike,
    },
    utils::Item,
};
use serde_json::json;

use super::utils::{assert_pyarrow, write_pyarrow, Result};

#[test]
fn fixed_shape_tensor() -> Result<()> {
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

    assert_pyarrow(
        "fixed_shape_tensor.ipc",
        &batch,
        r#"
        import sys
        import pyarrow as pa

        tbl = pa.ipc.open_file(sys.argv[1]).read_all()
        col = tbl["item"].combine_chunks()

        assert tbl.num_rows == 4
        assert col.type.extension_name == "arrow.fixed_shape_tensor"
        assert col.type.value_type == pa.int64()
        assert col.type.shape == [3, 2, 1]
        assert col.type.dim_names is None
        assert col.type.permutation is None
        assert str(col.storage.type) == "fixed_size_list<item: int64>[6]"
        assert col.to_pylist() == [
            [1, 2, 3, 4, 5, 6],
            [7, 8, 9, 0, 1, 2],
            [3, 4, 5, 6, 7, 8],
            [9, 0, 1, 2, 3, 4],
        ]
        assert col.to_numpy_ndarray().shape == (4, 3, 2, 1)
        assert col.to_numpy_ndarray().tolist() == [
            [[[1], [2]], [[3], [4]], [[5], [6]]],
            [[[7], [8]], [[9], [0]], [[1], [2]]],
            [[[3], [4]], [[5], [6]], [[7], [8]]],
            [[[9], [0]], [[1], [2]], [[3], [4]]],
        ]
    "#,
    )
}

#[test]
fn variable_shape_tensor() -> Result<()> {
    #[derive(Serialize)]
    struct Tensor {
        data: Vec<i64>,
        shape: Vec<i32>,
    }

    let items = vec![
        Item(Tensor {
            data: vec![1, 2, 3, 4, 5, 6],
            shape: vec![3, 2, 1],
        }),
        Item(Tensor {
            data: vec![1, 2],
            shape: vec![2, 1, 1],
        }),
        Item(Tensor {
            data: vec![1, 2, 3, 4],
            shape: vec![2, 2, 1],
        }),
    ];
    let fields = Vec::<FieldRef>::from_value(&[VariableShapeTensorField::new(
        "item",
        json!({"name": "element", "data_type": "I64"}),
        3,
    )?])?;

    let batch = serde_arrow::to_record_batch(&fields, &items)?;

    assert_pyarrow(
        "variable_shape_tensor.ipc",
        &batch,
        r#"
        import sys
        import pyarrow as pa

        tbl = pa.ipc.open_file(sys.argv[1]).read_all()
        col = tbl["item"].combine_chunks()

        assert tbl.num_rows == 3
        assert col.type.extension_name == "arrow.variable_shape_tensor"
        assert str(col.type) == "extension<arrow.variable_shape_tensor[value_type=int64, ndim=3]>"
        assert str(col.storage.type) == "struct<data: list<item: int64>, shape: fixed_size_list<item: int32>[3]>"
        assert col.storage.type[0].name == "data"
        assert col.storage.type[1].name == "shape"
        assert col.to_pylist() == [
            {"data": [1, 2, 3, 4, 5, 6], "shape": [3, 2, 1]},
            {"data": [1, 2], "shape": [2, 1, 1]},
            {"data": [1, 2, 3, 4], "shape": [2, 2, 1]},
        ]
    "#,
    )
}

#[test]
fn fixed_shape_tensor_with_metadata_options() -> Result<()> {
    let items = vec![
        Item(vec![1.0_f32, 2.0, 3.0, 4.0]),
        Item(vec![5.0, 6.0, 7.0, 8.0]),
    ];
    let fields = Vec::<FieldRef>::from_value(&[FixedShapeTensorField::new(
        "item",
        json!({"name": "element", "data_type": "F32"}),
        vec![2, 2],
    )?
    .dim_names(vec!["height".into(), "width".into()])?
    .permutation(vec![1, 0])?])?;

    let batch = serde_arrow::to_record_batch(&fields, &items)?;

    assert_pyarrow(
        "fixed_shape_tensor_with_metadata_options.ipc",
        &batch,
        r#"
        import sys
        import pyarrow as pa

        tbl = pa.ipc.open_file(sys.argv[1]).read_all()
        col = tbl["item"].combine_chunks()

        assert col.type.extension_name == "arrow.fixed_shape_tensor"
        assert col.type.value_type == pa.float32()
        assert col.type.shape == [2, 2]
        assert col.type.dim_names == ["height", "width"]
        assert col.type.permutation == [1, 0]
        assert str(col.storage.type) == "fixed_size_list<item: float>[4]"
        assert col.to_pylist() == [
            [1.0, 2.0, 3.0, 4.0],
            [5.0, 6.0, 7.0, 8.0],
        ]
    "#,
    )
}

#[test]
fn variable_shape_tensor_with_metadata_options() -> Result<()> {
    #[derive(Serialize)]
    struct Tensor {
        data: Vec<f64>,
        shape: Vec<i32>,
    }

    let items = vec![
        Item(Tensor {
            data: vec![1.0, 2.0, 3.0, 4.0],
            shape: vec![2, 2, 1],
        }),
        Item(Tensor {
            data: vec![5.0, 6.0],
            shape: vec![1, 2, 1],
        }),
    ];
    let fields = Vec::<FieldRef>::from_value(&[VariableShapeTensorField::new(
        "item",
        json!({"name": "element", "data_type": "F64"}),
        3,
    )?
    .dim_names(vec!["batch".into(), "height".into(), "width".into()])?
    .permutation(vec![2, 1, 0])?
    .uniform_shape(vec![None, Some(2), Some(1)])?])?;

    let batch = serde_arrow::to_record_batch(&fields, &items)?;

    assert_pyarrow(
        "variable_shape_tensor_with_metadata_options.ipc",
        &batch,
        r#"
        import sys
        import pyarrow as pa

        tbl = pa.ipc.open_file(sys.argv[1]).read_all()
        col = tbl["item"].combine_chunks()

        assert col.type.extension_name == "arrow.variable_shape_tensor"
        assert "value_type=double" in str(col.type)
        assert "ndim=3" in str(col.type)
        assert str(col.storage.type) == "struct<data: list<item: double>, shape: fixed_size_list<item: int32>[3]>"
        assert col.to_pylist() == [
            {"data": [1.0, 2.0, 3.0, 4.0], "shape": [2, 2, 1]},
            {"data": [5.0, 6.0], "shape": [1, 2, 1]},
        ]
    "#,
    )
}

#[test]
fn nullable_tensor_fields() -> Result<()> {
    #[derive(Serialize)]
    struct Tensor {
        data: Vec<i64>,
        shape: Vec<i32>,
    }

    #[derive(Serialize)]
    struct Record {
        fixed: Option<Vec<i64>>,
        variable: Option<Tensor>,
    }

    let items = vec![
        Record {
            fixed: Some(vec![1, 2, 3, 4]),
            variable: Some(Tensor {
                data: vec![1, 2, 3, 4],
                shape: vec![2, 2],
            }),
        },
        Record {
            fixed: None,
            variable: None,
        },
        Record {
            fixed: Some(vec![5, 6, 7, 8]),
            variable: Some(Tensor {
                data: vec![5, 6],
                shape: vec![1, 2],
            }),
        },
    ];
    let mut fields = Vec::<FieldRef>::from_value(&[FixedShapeTensorField::new(
        "fixed",
        json!({"name": "element", "data_type": "I64"}),
        vec![2, 2],
    )?
    .nullable(true)])?;
    fields.extend(Vec::<FieldRef>::from_value(&[
        VariableShapeTensorField::new(
            "variable",
            json!({"name": "element", "data_type": "I64"}),
            2,
        )?
        .nullable(true),
    ])?);

    let batch = serde_arrow::to_record_batch(&fields, &items)?;

    assert_pyarrow(
        "nullable_tensor_fields.ipc",
        &batch,
        r#"
        import sys
        import pyarrow as pa

        tbl = pa.ipc.open_file(sys.argv[1]).read_all()
        fixed = tbl["fixed"].combine_chunks()
        variable = tbl["variable"].combine_chunks()

        assert tbl.schema.field("fixed").nullable
        assert tbl.schema.field("variable").nullable
        assert fixed.type.extension_name == "arrow.fixed_shape_tensor"
        assert variable.type.extension_name == "arrow.variable_shape_tensor"
        assert fixed.is_null().to_pylist() == [False, True, False]
        assert variable.is_null().to_pylist() == [False, True, False]
        assert fixed.to_pylist() == [
            [1, 2, 3, 4],
            None,
            [5, 6, 7, 8],
        ]
        assert variable.to_pylist() == [
            {"data": [1, 2, 3, 4], "shape": [2, 2]},
            None,
            {"data": [5, 6], "shape": [1, 2]},
        ]
    "#,
    )
}

#[test]
fn pyarrow_fixed_shape_tensor_to_rust() -> Result<()> {
    let batch = write_pyarrow(
        "pyarrow_fixed_shape_tensor.ipc",
        r#"
        import sys
        import numpy as np
        import pyarrow as pa

        values = np.array(
            [
                [[[1], [2]], [[3], [4]], [[5], [6]]],
                [[[7], [8]], [[9], [0]], [[1], [2]]],
                [[[3], [4]], [[5], [6]], [[7], [8]]],
                [[[9], [0]], [[1], [2]], [[3], [4]]],
            ],
            dtype=np.int64,
        )
        tensor = pa.FixedShapeTensorArray.from_numpy_ndarray(values)
        tbl = pa.table({"item": tensor})

        with pa.OSFile(sys.argv[1], "wb") as sink:
            with pa.ipc.new_file(sink, tbl.schema) as writer:
                writer.write_table(tbl)
    "#,
    )?;

    let actual: Vec<Item<Vec<i64>>> = serde_arrow::from_record_batch(&batch)?;
    assert_eq!(
        actual,
        vec![
            Item(vec![1, 2, 3, 4, 5, 6]),
            Item(vec![7, 8, 9, 0, 1, 2]),
            Item(vec![3, 4, 5, 6, 7, 8]),
            Item(vec![9, 0, 1, 2, 3, 4]),
        ]
    );

    Ok(())
}
