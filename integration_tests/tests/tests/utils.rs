use std::{fs::File, path::PathBuf, process::Command};

use arrow::array::RecordBatch;

pub type Result<T, E = serde_arrow::_impl::PanicOnErrorError> = std::result::Result<T, E>;

pub fn write_file(name: &str, batch: &RecordBatch) -> Result<()> {
    let tmp_dir = PathBuf::from(env!("CARGO_TARGET_TMPDIR"));
    let file_path = tmp_dir.join(name);

    let file = File::create(&file_path)?;
    let mut writer = arrow::ipc::writer::FileWriter::try_new(file, &batch.schema())?;
    writer.write(&batch)?;
    writer.finish()?;
    Ok(())
}

pub fn execute_python(source: &str) -> Result<String> {
    let tmp_dir = PathBuf::from(env!("CARGO_TARGET_TMPDIR"));

    // TODO: implement proper dedent logic
    let mut dedented_source = String::new();
    for line in source.lines() {
        dedented_source.push_str(line.trim_start());
        dedented_source.push('\n');
    }
    let output = Command::new("python")
        .arg("-c")
        .arg(dedented_source)
        .current_dir(tmp_dir)
        .output()?;

    if !output.status.success() {
        panic!("command failed: {output:?}");
    }

    Ok(String::from_utf8(output.stdout)?)
}
