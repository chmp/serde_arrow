use serde::Deserialize;

use super::utils::{write_pyarrow, Result};

#[test]
fn pyarrow_interval_columns_are_reported_as_unsupported() -> Result<()> {
    #[derive(Debug, Deserialize)]
    struct Record {
        #[allow(dead_code)]
        interval: String,
    }

    let batch = write_pyarrow(
        "pyarrow_interval_columns.ipc",
        r#"
        import sys
        import pyarrow as pa

        schema = pa.schema([
            pa.field("interval", pa.month_day_nano_interval(), nullable=True),
        ])
        tbl = pa.table(
            [
                pa.array([(1, 2, 3), None, (4, 5, 6)], type=schema.field("interval").type),
            ],
            schema=schema,
        )
        with pa.OSFile(sys.argv[1], "wb") as sink:
            with pa.ipc.new_file(sink, tbl.schema) as writer:
                writer.write_table(tbl)
    "#,
    )?;

    assert!(
        format!("{:?}", batch.schema().field(0).data_type()).contains("Interval"),
        "expected an Arrow interval field, got {:?}",
        batch.schema().field(0).data_type(),
    );

    let err = serde_arrow::from_record_batch::<Vec<Record>>(&batch).unwrap_err();
    let err = err.to_string();
    assert!(
        err.contains("array view does not expose a length")
            || err.contains("Interval")
            || err.contains("MonthDayNano"),
        "unexpected error: {err}"
    );

    Ok(())
}
