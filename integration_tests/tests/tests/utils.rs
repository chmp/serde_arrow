use std::{
    env,
    fs::{self, File},
    path::{Path, PathBuf},
    process::Command,
};

use arrow::array::RecordBatch;

pub type Result<T> = std::result::Result<T, Box<dyn std::error::Error>>;

pub fn write_file(name: &str, batch: &RecordBatch) -> Result<PathBuf> {
    let tmp_dir = PathBuf::from(env!("CARGO_TARGET_TMPDIR"));
    let file_path = tmp_dir.join(name);

    fs::create_dir_all(&tmp_dir)?;
    let file = File::create(&file_path)?;
    let mut writer = arrow::ipc::writer::FileWriter::try_new(file, &batch.schema())?;
    writer.write(batch)?;
    writer.finish()?;
    Ok(file_path)
}

pub fn assert_pyarrow(batch_name: &str, batch: &RecordBatch, source: &str) -> Result<()> {
    let path = write_file(batch_name, batch)?;
    let _output = execute_python(source, &[&path])?;
    Ok(())
}

pub fn execute_python(source: &str, args: &[&Path]) -> Result<String> {
    let tmp_dir = PathBuf::from(env!("CARGO_TARGET_TMPDIR"));
    fs::create_dir_all(&tmp_dir)?;

    let python = env::var("SERDE_ARROW_PYTHON").unwrap_or_else(|_| "python".into());
    let script = dedent(source);
    let output = Command::new(&python)
        .arg("-c")
        .arg(&script)
        .args(args)
        .current_dir(tmp_dir)
        .output()
        .map_err(|err| {
            std::io::Error::new(
                err.kind(),
                format!("failed to run Python interpreter {python:?}: {err}"),
            )
        })?;

    if !output.status.success() {
        return Err(std::io::Error::other(format!(
            "Python command failed with status {}\nscript:\n{}\nstdout:\n{}\nstderr:\n{}",
            output.status,
            script,
            String::from_utf8_lossy(&output.stdout),
            String::from_utf8_lossy(&output.stderr),
        ))
        .into());
    }

    Ok(String::from_utf8(output.stdout)?)
}

fn dedent(source: &str) -> String {
    let lines = source.lines().collect::<Vec<_>>();
    let first = lines.iter().position(|line| !line.trim().is_empty());
    let last = lines.iter().rposition(|line| !line.trim().is_empty());
    let Some((first, last)) = first.zip(last) else {
        return String::new();
    };

    let indent = lines[first..=last]
        .iter()
        .filter(|line| !line.trim().is_empty())
        .map(|line| line.len() - line.trim_start().len())
        .min()
        .unwrap_or(0);

    let mut res = String::new();
    for line in &lines[first..=last] {
        res.push_str(line.get(indent..).unwrap_or(line));
        res.push('\n');
    }
    res
}
