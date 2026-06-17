use serde::Serialize;
use serde_arrow::schema::SchemaLike;
use serde_json::json;

use super::utils::{assert_pyarrow, Result};

#[test]
fn list_columns() -> Result<()> {
    #[derive(Serialize)]
    struct Record {
        id: i64,
        values: Vec<i32>,
        maybe_values: Option<Vec<Option<i32>>>,
    }

    let items = vec![
        Record {
            id: 1,
            values: vec![1, 2, 3],
            maybe_values: Some(vec![Some(10), None, Some(30)]),
        },
        Record {
            id: 2,
            values: Vec::new(),
            maybe_values: None,
        },
        Record {
            id: 3,
            values: vec![4, 5],
            maybe_values: Some(vec![None, Some(50)]),
        },
    ];
    let fields = Vec::from_value(json!([
        {"name": "id", "data_type": "I64"},
        {
            "name": "values",
            "data_type": "List",
            "children": [{"name": "element", "data_type": "I32"}],
        },
        {
            "name": "maybe_values",
            "data_type": "List",
            "nullable": true,
            "children": [{"name": "element", "data_type": "I32", "nullable": true}],
        },
    ]))?;

    let batch = serde_arrow::to_record_batch(&fields, &items)?;

    assert_pyarrow(
        "list_columns.ipc",
        &batch,
        r#"
        import sys
        import pyarrow as pa

        tbl = pa.ipc.open_file(sys.argv[1]).read_all()

        assert tbl.num_rows == 3
        assert tbl.schema.names == ["id", "values", "maybe_values"]
        assert tbl.schema.field("id").type == pa.int64()
        assert tbl.schema.field("values").type == pa.list_(pa.field("element", pa.int32(), nullable=False))
        assert tbl.schema.field("maybe_values").nullable
        assert tbl.schema.field("maybe_values").type == pa.list_(pa.field("element", pa.int32(), nullable=True))

        assert tbl["id"].to_pylist() == [1, 2, 3]
        assert tbl["values"].to_pylist() == [[1, 2, 3], [], [4, 5]]
        assert tbl["maybe_values"].to_pylist() == [
            [10, None, 30],
            None,
            [None, 50],
        ]
    "#,
    )
}

#[test]
fn struct_columns_and_list_of_structs() -> Result<()> {
    #[derive(Serialize)]
    struct Point {
        x: f64,
        y: f64,
    }

    #[derive(Serialize)]
    struct Measurement {
        name: String,
        value: i64,
    }

    #[derive(Serialize)]
    struct Record {
        id: i64,
        point: Point,
        maybe_point: Option<Point>,
        measurements: Vec<Measurement>,
    }

    let items = vec![
        Record {
            id: 1,
            point: Point { x: 1.5, y: 2.5 },
            maybe_point: Some(Point { x: 3.5, y: 4.5 }),
            measurements: vec![
                Measurement {
                    name: "a".into(),
                    value: 10,
                },
                Measurement {
                    name: "b".into(),
                    value: 20,
                },
            ],
        },
        Record {
            id: 2,
            point: Point { x: -1.0, y: -2.0 },
            maybe_point: None,
            measurements: Vec::new(),
        },
        Record {
            id: 3,
            point: Point { x: 0.25, y: 0.5 },
            maybe_point: Some(Point { x: 8.0, y: 9.0 }),
            measurements: vec![Measurement {
                name: "c".into(),
                value: 30,
            }],
        },
    ];
    let fields = Vec::from_value(json!([
        {"name": "id", "data_type": "I64"},
        {
            "name": "point",
            "data_type": "Struct",
            "children": [
                {"name": "x", "data_type": "F64"},
                {"name": "y", "data_type": "F64"},
            ],
        },
        {
            "name": "maybe_point",
            "data_type": "Struct",
            "nullable": true,
            "children": [
                {"name": "x", "data_type": "F64"},
                {"name": "y", "data_type": "F64"},
            ],
        },
        {
            "name": "measurements",
            "data_type": "List",
            "children": [{
                "name": "element",
                "data_type": "Struct",
                "children": [
                    {"name": "name", "data_type": "Utf8"},
                    {"name": "value", "data_type": "I64"},
                ],
            }],
        },
    ]))?;

    let batch = serde_arrow::to_record_batch(&fields, &items)?;

    assert_pyarrow(
        "struct_columns_and_list_of_structs.ipc",
        &batch,
        r#"
        import sys
        import pyarrow as pa

        tbl = pa.ipc.open_file(sys.argv[1]).read_all()
        point_type = pa.struct([
            pa.field("x", pa.float64(), nullable=False),
            pa.field("y", pa.float64(), nullable=False),
        ])
        measurement_type = pa.list_(
            pa.field(
                "element",
                pa.struct([
                    pa.field("name", pa.string(), nullable=False),
                    pa.field("value", pa.int64(), nullable=False),
                ]),
                nullable=False,
            )
        )

        assert tbl.num_rows == 3
        assert tbl.schema.names == ["id", "point", "maybe_point", "measurements"]
        assert tbl.schema.field("point").type == point_type
        assert not tbl.schema.field("point").nullable
        assert tbl.schema.field("maybe_point").type == point_type
        assert tbl.schema.field("maybe_point").nullable
        assert tbl.schema.field("measurements").type == measurement_type

        assert tbl["point"].to_pylist() == [
            {"x": 1.5, "y": 2.5},
            {"x": -1.0, "y": -2.0},
            {"x": 0.25, "y": 0.5},
        ]
        assert tbl["maybe_point"].to_pylist() == [
            {"x": 3.5, "y": 4.5},
            None,
            {"x": 8.0, "y": 9.0},
        ]
        assert tbl["maybe_point"].combine_chunks().is_null().to_pylist() == [
            False,
            True,
            False,
        ]
        assert tbl["measurements"].to_pylist() == [
            [{"name": "a", "value": 10}, {"name": "b", "value": 20}],
            [],
            [{"name": "c", "value": 30}],
        ]
    "#,
    )
}
