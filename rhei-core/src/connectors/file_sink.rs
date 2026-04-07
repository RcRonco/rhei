use std::fs::File;
use std::io::{BufWriter, Write};
use std::marker::PhantomData;
use std::path::{Path, PathBuf};

use async_trait::async_trait;
use serde::Serialize;

use crate::traits::Sink;

/// A sink that writes each element as a JSON line to a file.
///
/// Each element is serialized with `serde_json` and written as a single line,
/// producing a [JSON Lines](https://jsonlines.org/) file.
#[derive(Debug)]
pub struct FileSink<T> {
    writer: BufWriter<File>,
    path: PathBuf,
    _marker: PhantomData<T>,
}

impl<T> FileSink<T> {
    /// Creates a new `FileSink` that writes to the given path.
    ///
    /// The file is created (or truncated) immediately.
    pub fn new(path: impl AsRef<Path>) -> anyhow::Result<Self> {
        let path = path.as_ref().to_path_buf();
        let file = File::create(&path)?;
        Ok(Self {
            writer: BufWriter::new(file),
            path,
            _marker: PhantomData,
        })
    }

    /// Returns the path this sink writes to.
    pub fn path(&self) -> &Path {
        &self.path
    }
}

#[async_trait]
impl<T: Send + Sync + Serialize + 'static> Sink for FileSink<T> {
    type Input = T;

    async fn write(&mut self, input: T) -> anyhow::Result<()> {
        serde_json::to_writer(&mut self.writer, &input)?;
        self.writer.write_all(b"\n")?;
        Ok(())
    }

    async fn flush(&mut self) -> anyhow::Result<()> {
        self.writer.flush()?;
        Ok(())
    }
}

#[cfg(test)]
#[allow(clippy::unwrap_used, clippy::expect_used)]
mod tests {
    use super::*;

    #[derive(Debug, Clone, serde::Serialize, serde::Deserialize, PartialEq)]
    struct Record {
        name: String,
        value: i32,
    }

    #[tokio::test]
    async fn writes_json_lines() {
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("output.jsonl");

        let mut sink = FileSink::<Record>::new(&path).unwrap();
        sink.write(Record {
            name: "alice".into(),
            value: 1,
        })
        .await
        .unwrap();
        sink.write(Record {
            name: "bob".into(),
            value: 2,
        })
        .await
        .unwrap();
        sink.flush().await.unwrap();

        let content = std::fs::read_to_string(&path).unwrap();
        let lines: Vec<&str> = content.lines().collect();
        assert_eq!(lines.len(), 2);

        let r0: Record = serde_json::from_str(lines[0]).unwrap();
        assert_eq!(r0.name, "alice");
        assert_eq!(r0.value, 1);

        let r1: Record = serde_json::from_str(lines[1]).unwrap();
        assert_eq!(r1.name, "bob");
        assert_eq!(r1.value, 2);
    }

    #[tokio::test]
    async fn empty_sink_creates_empty_file() {
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("empty.jsonl");

        let sink = FileSink::<Record>::new(&path).unwrap();
        drop(sink);

        let content = std::fs::read_to_string(&path).unwrap();
        assert!(content.is_empty());
    }
}
