//! File-backed dead-letter queue sink.
//!
//! Writes [`DeadLetterRecord`]s as JSON lines to a file.

use std::fs::{File, OpenOptions};
use std::io::{BufWriter, Write};
use std::path::{Path, PathBuf};

use crate::dlq::DeadLetterRecord;

/// Appends [`DeadLetterRecord`]s as JSON lines to a file.
#[derive(Debug)]
pub struct DlqFileSink {
    writer: BufWriter<File>,
    path: PathBuf,
}

impl DlqFileSink {
    /// Open (or create) the DLQ file at the given path.
    pub fn open(path: impl AsRef<Path>) -> std::io::Result<Self> {
        let path = path.as_ref().to_path_buf();
        let file = OpenOptions::new().create(true).append(true).open(&path)?;
        Ok(Self {
            writer: BufWriter::new(file),
            path,
        })
    }

    /// Write a dead-letter record as a JSON line.
    pub fn write_record(&mut self, record: &DeadLetterRecord) -> std::io::Result<()> {
        let json = serde_json::to_string(record).map_err(std::io::Error::other)?;
        self.writer.write_all(json.as_bytes())?;
        self.writer.write_all(b"\n")?;
        Ok(())
    }

    /// Flush buffered writes to disk.
    pub fn flush(&mut self) -> std::io::Result<()> {
        self.writer.flush()
    }

    /// Returns the path to the DLQ file.
    pub fn path(&self) -> &Path {
        &self.path
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::dlq::DeadLetterRecord;

    #[test]
    fn writes_and_reads_records() {
        let dir = std::env::temp_dir().join(format!("rill_dlq_test_{}", std::process::id()));
        let _ = std::fs::remove_dir_all(&dir);
        std::fs::create_dir_all(&dir).unwrap();
        let path = dir.join("dlq.jsonl");

        let record = DeadLetterRecord {
            input_repr: r#"ClickEvent { user: "alice" }"#.to_string(),
            operator_name: "click_counter".to_string(),
            error: "state backend unavailable".to_string(),
            timestamp: "2026-01-01T00:00:00Z".to_string(),
        };

        {
            let mut sink = DlqFileSink::open(&path).unwrap();
            sink.write_record(&record).unwrap();
            sink.write_record(&record).unwrap();
            sink.flush().unwrap();
        }

        let content = std::fs::read_to_string(&path).unwrap();
        let lines: Vec<&str> = content.lines().collect();
        assert_eq!(lines.len(), 2);
        let parsed: DeadLetterRecord = serde_json::from_str(lines[0]).unwrap();
        assert_eq!(parsed.operator_name, "click_counter");

        let _ = std::fs::remove_dir_all(&dir);
    }
}
