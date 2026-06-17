use serde::{Deserialize, Serialize};
use serde_arrow::schema::SchemaLike;
use serde_json::json;

use super::utils::{assert_pyarrow, write_pyarrow, Result};

#[test]
fn rust_temporal_columns_to_pyarrow() -> Result<()> {
    #[derive(Serialize)]
    struct Record {
        date32: Option<String>,
        date64: Option<String>,
        time32s: Option<String>,
        time64us: Option<String>,
        timestamp_ms: Option<String>,
        timestamp_utc_ms: Option<String>,
        duration_ms: Option<i64>,
    }

    let items = vec![
        Record {
            date32: Some("2025-01-20".into()),
            date64: Some("2025-01-20".into()),
            time32s: Some("12:00:00".into()),
            time64us: Some("12:00:00.123456".into()),
            timestamp_ms: Some("2025-01-20T19:30:42.123".into()),
            timestamp_utc_ms: Some("2025-01-20T19:30:42.123Z".into()),
            duration_ms: Some(1234),
        },
        Record {
            date32: None,
            date64: None,
            time32s: None,
            time64us: None,
            timestamp_ms: None,
            timestamp_utc_ms: None,
            duration_ms: None,
        },
        Record {
            date32: Some("1970-01-01".into()),
            date64: Some("1970-01-01".into()),
            time32s: Some("23:59:59".into()),
            time64us: Some("23:59:59".into()),
            timestamp_ms: Some("1970-01-01T00:00:00".into()),
            timestamp_utc_ms: Some("1970-01-01T00:00:00Z".into()),
            duration_ms: Some(-5),
        },
    ];
    let fields = Vec::from_value(json!([
        {"name": "date32", "data_type": "Date32", "nullable": true},
        {"name": "date64", "data_type": "Date64", "nullable": true},
        {"name": "time32s", "data_type": "Time32(Second)", "nullable": true},
        {"name": "time64us", "data_type": "Time64(Microsecond)", "nullable": true},
        {"name": "timestamp_ms", "data_type": "Timestamp(Millisecond, None)", "nullable": true},
        {"name": "timestamp_utc_ms", "data_type": "Timestamp(Millisecond, Some(\"UTC\"))", "nullable": true},
        {"name": "duration_ms", "data_type": "Duration(Millisecond)", "nullable": true},
    ]))?;

    let batch = serde_arrow::to_record_batch(&fields, &items)?;

    assert_pyarrow(
        "rust_temporal_columns.ipc",
        &batch,
        r#"
        import sys
        import pyarrow as pa
        from datetime import date, time, datetime, timezone, timedelta

        tbl = pa.ipc.open_file(sys.argv[1]).read_all()

        assert tbl.schema.field("date32").type == pa.date32()
        assert tbl.schema.field("date64").type == pa.date64()
        assert tbl.schema.field("time32s").type == pa.time32("s")
        assert tbl.schema.field("time64us").type == pa.time64("us")
        assert tbl.schema.field("timestamp_ms").type == pa.timestamp("ms")
        assert tbl.schema.field("timestamp_utc_ms").type == pa.timestamp("ms", tz="UTC")
        assert tbl.schema.field("duration_ms").type == pa.duration("ms")

        assert tbl["date32"].to_pylist() == [date(2025, 1, 20), None, date(1970, 1, 1)]
        assert tbl["date64"].to_pylist() == [date(2025, 1, 20), None, date(1970, 1, 1)]
        assert tbl["time32s"].to_pylist() == [time(12, 0), None, time(23, 59, 59)]
        assert tbl["time64us"].to_pylist() == [time(12, 0, 0, 123456), None, time(23, 59, 59)]
        assert tbl["timestamp_ms"].to_pylist() == [
            datetime(2025, 1, 20, 19, 30, 42, 123000),
            None,
            datetime(1970, 1, 1),
        ]
        assert tbl["timestamp_utc_ms"].to_pylist() == [
            datetime(2025, 1, 20, 19, 30, 42, 123000, tzinfo=timezone.utc),
            None,
            datetime(1970, 1, 1, tzinfo=timezone.utc),
        ]
        assert tbl["duration_ms"].to_pylist() == [
            timedelta(seconds=1, microseconds=234000),
            None,
            timedelta(milliseconds=-5),
        ]
    "#,
    )
}

#[test]
fn pyarrow_temporal_columns_to_rust() -> Result<()> {
    #[derive(Debug, Deserialize, PartialEq)]
    struct Record {
        date32: Option<String>,
        date64: Option<String>,
        time32s: Option<String>,
        time64us: Option<String>,
        timestamp_ms: Option<String>,
        timestamp_utc_ms: Option<String>,
        duration_ms: Option<i64>,
    }

    let batch = write_pyarrow(
        "pyarrow_temporal_columns.ipc",
        r#"
        import sys
        import pyarrow as pa
        from datetime import date, time, datetime, timezone, timedelta

        schema = pa.schema([
            pa.field("date32", pa.date32(), nullable=True),
            pa.field("date64", pa.date64(), nullable=True),
            pa.field("time32s", pa.time32("s"), nullable=True),
            pa.field("time64us", pa.time64("us"), nullable=True),
            pa.field("timestamp_ms", pa.timestamp("ms"), nullable=True),
            pa.field("timestamp_utc_ms", pa.timestamp("ms", tz="UTC"), nullable=True),
            pa.field("duration_ms", pa.duration("ms"), nullable=True),
        ])
        tbl = pa.table(
            [
                pa.array([date(2025, 1, 20), None, date(1970, 1, 1)], type=schema.field("date32").type),
                pa.array([date(2025, 1, 20), None, date(1970, 1, 1)], type=schema.field("date64").type),
                pa.array([time(12, 0), None, time(23, 59, 59)], type=schema.field("time32s").type),
                pa.array([time(12, 0, 0, 123456), None, time(23, 59, 59)], type=schema.field("time64us").type),
                pa.array(
                    [datetime(2025, 1, 20, 19, 30, 42, 123000), None, datetime(1970, 1, 1)],
                    type=schema.field("timestamp_ms").type,
                ),
                pa.array(
                    [
                        datetime(2025, 1, 20, 19, 30, 42, 123000, tzinfo=timezone.utc),
                        None,
                        datetime(1970, 1, 1, tzinfo=timezone.utc),
                    ],
                    type=schema.field("timestamp_utc_ms").type,
                ),
                pa.array(
                    [timedelta(seconds=1, milliseconds=234), None, timedelta(milliseconds=-5)],
                    type=schema.field("duration_ms").type,
                ),
            ],
            schema=schema,
        )
        with pa.OSFile(sys.argv[1], "wb") as sink:
            with pa.ipc.new_file(sink, tbl.schema) as writer:
                writer.write_table(tbl)
    "#,
    )?;

    let actual: Vec<Record> = serde_arrow::from_record_batch(&batch)?;
    assert_eq!(
        actual,
        vec![
            Record {
                date32: Some("2025-01-20".into()),
                date64: Some("2025-01-20".into()),
                time32s: Some("12:00:00".into()),
                time64us: Some("12:00:00.123456".into()),
                timestamp_ms: Some("2025-01-20T19:30:42.123".into()),
                timestamp_utc_ms: Some("2025-01-20T19:30:42.123Z".into()),
                duration_ms: Some(1234),
            },
            Record {
                date32: None,
                date64: None,
                time32s: None,
                time64us: None,
                timestamp_ms: None,
                timestamp_utc_ms: None,
                duration_ms: None,
            },
            Record {
                date32: Some("1970-01-01".into()),
                date64: Some("1970-01-01".into()),
                time32s: Some("23:59:59".into()),
                time64us: Some("23:59:59".into()),
                timestamp_ms: Some("1970-01-01T00:00:00".into()),
                timestamp_utc_ms: Some("1970-01-01T00:00:00Z".into()),
                duration_ms: Some(-5),
            },
        ]
    );

    Ok(())
}
