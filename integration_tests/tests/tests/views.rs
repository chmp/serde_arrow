use serde::{Deserialize, Serialize};
use serde_arrow::schema::SchemaLike;
use serde_json::json;

use super::utils::{execute_python, read_file, write_file, Result};

#[test]
fn rust_view_columns_to_pyarrow() -> Result<()> {
    #[derive(Serialize)]
    struct Record {
        text: Option<String>,
        bytes: Option<Vec<u8>>,
    }

    let items = vec![
        Record {
            text: Some("short".into()),
            bytes: Some(b"abc".to_vec()),
        },
        Record {
            text: Some("a string long enough to require external storage".into()),
            bytes: Some(b"a byte string long enough to require external storage".to_vec()),
        },
        Record {
            text: None,
            bytes: None,
        },
    ];
    let fields = Vec::from_value(json!([
        {"name": "text", "data_type": "Utf8View", "nullable": true},
        {"name": "bytes", "data_type": "BinaryView", "nullable": true},
    ]))?;

    let batch = serde_arrow::to_record_batch(&fields, &items)?;

    write_file("rust_view_columns.ipc", &batch)?;
    let _output = execute_python(
        r#"
        import sys
        import pyarrow as pa

        tbl = pa.ipc.open_file(sys.argv[1]).read_all()

        assert tbl.schema.field("text").type == pa.string_view()
        assert tbl.schema.field("bytes").type == pa.binary_view()
        assert tbl["text"].to_pylist() == [
            "short",
            "a string long enough to require external storage",
            None,
        ]
        assert tbl["bytes"].to_pylist() == [
            b"abc",
            b"a byte string long enough to require external storage",
            None,
        ]
    "#,
        &["rust_view_columns.ipc"],
    )?;

    Ok(())
}

#[test]
fn pyarrow_view_columns_to_rust() -> Result<()> {
    #[derive(Debug, Deserialize, PartialEq)]
    struct Record {
        text: Option<String>,
        bytes: Option<Vec<u8>>,
    }

    let _output = execute_python(
        r#"
        import sys
        import pyarrow as pa

        schema = pa.schema([
            pa.field("text", pa.string_view(), nullable=True),
            pa.field("bytes", pa.binary_view(), nullable=True),
        ])
        tbl = pa.table(
            [
                pa.array(
                    ["short", "a string long enough to require external storage", None],
                    type=schema.field("text").type,
                ),
                pa.array(
                    [b"abc", b"a byte string long enough to require external storage", None],
                    type=schema.field("bytes").type,
                ),
            ],
            schema=schema,
        )
        with pa.OSFile(sys.argv[1], "wb") as sink:
            with pa.ipc.new_file(sink, tbl.schema) as writer:
                writer.write_table(tbl)
    "#,
        &["pyarrow_view_columns.ipc"],
    )?;
    let batch = read_file("pyarrow_view_columns.ipc")?;

    let actual: Vec<Record> = serde_arrow::from_record_batch(&batch)?;
    assert_eq!(
        actual,
        vec![
            Record {
                text: Some("short".into()),
                bytes: Some(b"abc".to_vec()),
            },
            Record {
                text: Some("a string long enough to require external storage".into()),
                bytes: Some(b"a byte string long enough to require external storage".to_vec()),
            },
            Record {
                text: None,
                bytes: None,
            },
        ]
    );

    Ok(())
}
