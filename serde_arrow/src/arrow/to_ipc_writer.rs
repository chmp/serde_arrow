//! High level utilities
//!
use std::io::Write;

use arrow::ipc::writer::FileWriter;

use crate::{Result, Schema};

/// Helper to write an Arrow IPC file from a sequence of records
///
pub fn to_ipc_writer<W, T>(writer: W, value: &T, schema: &Schema) -> Result<()>
where
    W: Write,
    T: serde::Serialize + ?Sized,
{
    let batch = super::to_record_batch::to_record_batch(value, schema)?;
    let mut writer = FileWriter::try_new(writer, batch.schema().as_ref())?;

    writer.write(&batch)?;
    writer.finish()?;

    Ok(())
}
