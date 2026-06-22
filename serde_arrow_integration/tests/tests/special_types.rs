use std::collections::BTreeMap;

use serde::{Deserialize, Serialize};
use serde_arrow::schema::SchemaLike;
use serde_json::json;

use super::utils::{execute_python, read_file, write_file, Result};

#[test]
fn rust_dictionary_column_to_pyarrow() -> Result<()> {
    #[derive(Serialize)]
    struct Record {
        label: Option<String>,
    }

    let items = vec![
        Record {
            label: Some("red".into()),
        },
        Record { label: None },
        Record {
            label: Some("blue".into()),
        },
        Record {
            label: Some("red".into()),
        },
    ];
    let fields = Vec::from_value(json!([{
        "name": "label",
        "data_type": "Dictionary",
        "nullable": true,
        "children": [
            {"name": "key", "data_type": "I32"},
            {"name": "value", "data_type": "LargeUtf8"},
        ],
    }]))?;

    let batch = serde_arrow::to_record_batch(&fields, &items)?;

    write_file("rust_dictionary_column.ipc", &batch)?;
    let _output = execute_python(
        r#"
        import sys
        import pyarrow as pa

        tbl = pa.ipc.open_file(sys.argv[1]).read_all()
        col = tbl["label"].combine_chunks()

        assert col.type == pa.dictionary(pa.int32(), pa.large_string())
        assert col.to_pylist() == ["red", None, "blue", "red"]
        assert col.dictionary.to_pylist() == ["red", "blue"]
        assert col.indices.to_pylist() == [0, None, 1, 0]
    "#,
        &["rust_dictionary_column.ipc"],
    )?;

    Ok(())
}

#[test]
fn pyarrow_dictionary_column_to_rust() -> Result<()> {
    #[derive(Debug, Deserialize, PartialEq)]
    struct Record {
        label: Option<String>,
    }

    let _output = execute_python(
        r#"
        import sys
        import pyarrow as pa

        labels = pa.array(["red", None, "blue", "red"], type=pa.string()).dictionary_encode()
        tbl = pa.table({"label": labels})

        with pa.OSFile(sys.argv[1], "wb") as sink:
            with pa.ipc.new_file(sink, tbl.schema) as writer:
                writer.write_table(tbl)
    "#,
        &["pyarrow_dictionary_column.ipc"],
    )?;
    let batch = read_file("pyarrow_dictionary_column.ipc")?;

    let actual: Vec<Record> = serde_arrow::from_record_batch(&batch)?;
    assert_eq!(
        actual,
        vec![
            Record {
                label: Some("red".into())
            },
            Record { label: None },
            Record {
                label: Some("blue".into())
            },
            Record {
                label: Some("red".into())
            },
        ]
    );

    Ok(())
}

#[test]
fn rust_decimal_column_to_pyarrow() -> Result<()> {
    #[derive(Serialize)]
    struct Record {
        amount: Option<String>,
    }

    let items = vec![
        Record {
            amount: Some("1.23".into()),
        },
        Record { amount: None },
        Record {
            amount: Some("-45.67".into()),
        },
    ];
    let fields = Vec::from_value(json!([{
        "name": "amount",
        "data_type": "Decimal128(8, 2)",
        "nullable": true,
    }]))?;

    let batch = serde_arrow::to_record_batch(&fields, &items)?;

    write_file("rust_decimal_column.ipc", &batch)?;
    let _output = execute_python(
        r#"
        import sys
        import pyarrow as pa
        from decimal import Decimal

        tbl = pa.ipc.open_file(sys.argv[1]).read_all()
        col = tbl["amount"].combine_chunks()

        assert col.type == pa.decimal128(8, 2)
        assert col.to_pylist() == [Decimal("1.23"), None, Decimal("-45.67")]
    "#,
        &["rust_decimal_column.ipc"],
    )?;

    Ok(())
}

#[test]
fn pyarrow_decimal_column_to_rust() -> Result<()> {
    #[derive(Debug, Deserialize, PartialEq)]
    struct Record {
        amount: Option<String>,
    }

    let _output = execute_python(
        r#"
        import sys
        import pyarrow as pa
        from decimal import Decimal

        schema = pa.schema([
            pa.field("amount", pa.decimal128(8, 2), nullable=True),
        ])
        tbl = pa.table(
            [
                pa.array([Decimal("1.23"), None, Decimal("-45.67")], type=schema.field("amount").type),
            ],
            schema=schema,
        )

        with pa.OSFile(sys.argv[1], "wb") as sink:
            with pa.ipc.new_file(sink, tbl.schema) as writer:
                writer.write_table(tbl)
    "#,
        &["pyarrow_decimal_column.ipc"],
    )?;
    let batch = read_file("pyarrow_decimal_column.ipc")?;

    let actual: Vec<Record> = serde_arrow::from_record_batch(&batch)?;
    assert_eq!(
        actual,
        vec![
            Record {
                amount: Some("1.23".into())
            },
            Record { amount: None },
            Record {
                amount: Some("-45.67".into())
            },
        ]
    );

    Ok(())
}

#[test]
fn rust_map_column_to_pyarrow() -> Result<()> {
    #[derive(Serialize)]
    struct Record {
        attrs: Option<BTreeMap<String, u32>>,
    }

    let items = vec![
        Record {
            attrs: Some(BTreeMap::from([("a".into(), 1), ("b".into(), 2)])),
        },
        Record { attrs: None },
        Record {
            attrs: Some(BTreeMap::new()),
        },
        Record {
            attrs: Some(BTreeMap::from([("c".into(), 3)])),
        },
    ];
    let fields = Vec::from_value(json!([{
        "name": "attrs",
        "data_type": "Map",
        "nullable": true,
        "children": [{
            "name": "entries",
            "data_type": "Struct",
            "children": [
                {"name": "key", "data_type": "LargeUtf8"},
                {"name": "value", "data_type": "U32"},
            ],
        }],
    }]))?;

    let batch = serde_arrow::to_record_batch(&fields, &items)?;

    write_file("rust_map_column.ipc", &batch)?;
    let _output = execute_python(
        r#"
        import sys
        import pyarrow as pa

        tbl = pa.ipc.open_file(sys.argv[1]).read_all()
        col = tbl["attrs"].combine_chunks()

        assert str(col.type) == "map<large_string, uint32>"
        assert col.type.key_field == pa.field("key", pa.large_string(), nullable=False)
        assert col.type.item_field == pa.field("value", pa.uint32(), nullable=False)
        assert col.to_pylist() == [[("a", 1), ("b", 2)], None, [], [("c", 3)]]
    "#,
        &["rust_map_column.ipc"],
    )?;

    Ok(())
}

#[test]
fn pyarrow_map_column_to_rust() -> Result<()> {
    #[derive(Debug, Deserialize, PartialEq)]
    struct Record {
        attrs: Option<BTreeMap<String, u32>>,
    }

    let _output = execute_python(
        r#"
        import sys
        import pyarrow as pa

        schema = pa.schema([
            pa.field("attrs", pa.map_(pa.large_string(), pa.uint32()), nullable=True),
        ])
        tbl = pa.table(
            [
                pa.array(
                    [[("a", 1), ("b", 2)], None, [], [("c", 3)]],
                    type=schema.field("attrs").type,
                ),
            ],
            schema=schema,
        )

        with pa.OSFile(sys.argv[1], "wb") as sink:
            with pa.ipc.new_file(sink, tbl.schema) as writer:
                writer.write_table(tbl)
    "#,
        &["pyarrow_map_column.ipc"],
    )?;
    let batch = read_file("pyarrow_map_column.ipc")?;

    let actual: Vec<Record> = serde_arrow::from_record_batch(&batch)?;
    assert_eq!(
        actual,
        vec![
            Record {
                attrs: Some(BTreeMap::from([("a".into(), 1), ("b".into(), 2)])),
            },
            Record { attrs: None },
            Record {
                attrs: Some(BTreeMap::new()),
            },
            Record {
                attrs: Some(BTreeMap::from([("c".into(), 3)])),
            },
        ]
    );

    Ok(())
}
