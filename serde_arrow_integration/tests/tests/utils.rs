use std::{
    env,
    fs::{self, File},
    path::PathBuf,
    process::Command,
};

use arrow::array::RecordBatch;

pub type Result<T> = std::result::Result<T, Box<dyn std::error::Error>>;

pub fn write_file(name: &str, batch: &RecordBatch) -> Result<PathBuf> {
    let file_path = file_path(name)?;

    let file = File::create(&file_path)?;
    let mut writer = arrow::ipc::writer::FileWriter::try_new(file, &batch.schema())?;
    writer.write(batch)?;
    writer.finish()?;
    Ok(file_path)
}

pub fn read_file(name: &str) -> Result<RecordBatch> {
    let file = File::open(file_path(name)?)?;
    let mut reader = arrow::ipc::reader::FileReader::try_new(file, None)?;
    reader
        .next()
        .transpose()?
        .ok_or_else(|| std::io::Error::other("IPC file did not contain a record batch").into())
}

/// Run a Python snippet in the test temp directory.
///
/// Each entry in `paths` is interpreted relative to `CARGO_TARGET_TMPDIR` and
/// passed to Python as an absolute path via `sys.argv`.
pub fn execute_python(source: &str, paths: &[&str]) -> Result<String> {
    let tmp_dir = PathBuf::from(env!("CARGO_TARGET_TMPDIR"));
    fs::create_dir_all(&tmp_dir)?;

    let python = env::var("SERDE_ARROW_PYTHON").unwrap_or_else(|_| "python".into());
    let script = dedent(source);
    let paths = paths
        .iter()
        .map(|path| tmp_dir.join(path))
        .collect::<Vec<_>>();
    let output = Command::new(&python)
        .arg("-c")
        .arg(&script)
        .args(&paths)
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

fn file_path(name: &str) -> Result<PathBuf> {
    let tmp_dir = PathBuf::from(env!("CARGO_TARGET_TMPDIR"));
    fs::create_dir_all(&tmp_dir)?;
    Ok(tmp_dir.join(name))
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
