use std::io::Write;

use arrow2::io::ipc::write::{FileWriter, WriteOptions};

use super::to_chunk;
use crate::{Result, Schema};

pub fn write_ipc<W, T>(writer: W, value: &T, schema: &Schema) -> Result<()>
where
    W: Write,
    T: serde::Serialize + ?Sized,
{
    let arrow_schema = schema.try_into()?;
    let chunk = to_chunk(value, schema)?;

    let mut writer = FileWriter::try_new(writer, &arrow_schema, None, WriteOptions::default())?;
    writer.write(&chunk, None)?;
    writer.finish()?;

    Ok(())
}
