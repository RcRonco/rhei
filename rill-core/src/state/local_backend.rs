use std::collections::HashMap;
use std::path::PathBuf;
use std::sync::Mutex;
use std::time::Duration;

use async_trait::async_trait;

use super::backend::StateBackend;

/// A local file-backed state backend.
///
/// All state lives in an in-memory `HashMap`; `checkpoint()` serializes it to
/// a JSON file on disk. On construction, any existing checkpoint file is loaded.
///
/// An optional `latency` can be configured to simulate slow backends (e.g. S3)
/// during testing.
#[derive(Debug)]
pub struct LocalBackend {
    data: Mutex<HashMap<Vec<u8>, Vec<u8>>>,
    path: PathBuf,
    latency: Option<Duration>,
}

impl LocalBackend {
    /// Opens or creates a local backend at the given path, optionally with simulated latency.
    pub fn new(path: PathBuf, latency: Option<Duration>) -> anyhow::Result<Self> {
        let data = if path.exists() {
            let contents = std::fs::read_to_string(&path)?;
            let map: Vec<(Vec<u8>, Vec<u8>)> = serde_json::from_str(&contents)?;
            map.into_iter().collect()
        } else {
            HashMap::new()
        };

        Ok(Self {
            data: Mutex::new(data),
            path,
            latency,
        })
    }

    async fn maybe_sleep(&self) {
        if let Some(d) = self.latency {
            tokio::time::sleep(d).await;
        }
    }
}

#[async_trait]
impl StateBackend for LocalBackend {
    async fn get(&self, key: &[u8]) -> anyhow::Result<Option<Vec<u8>>> {
        self.maybe_sleep().await;
        let data = self.data.lock().unwrap();
        Ok(data.get(key).cloned())
    }

    async fn put(&self, key: &[u8], value: &[u8]) -> anyhow::Result<()> {
        self.maybe_sleep().await;
        let mut data = self.data.lock().unwrap();
        data.insert(key.to_vec(), value.to_vec());
        Ok(())
    }

    async fn delete(&self, key: &[u8]) -> anyhow::Result<()> {
        self.maybe_sleep().await;
        let mut data = self.data.lock().unwrap();
        data.remove(key);
        Ok(())
    }

    async fn checkpoint(&self) -> anyhow::Result<()> {
        let snapshot: Vec<(Vec<u8>, Vec<u8>)> = {
            let data = self.data.lock().unwrap();
            data.iter().map(|(k, v)| (k.clone(), v.clone())).collect()
        };
        let json = serde_json::to_string(&snapshot)?;
        if let Some(parent) = self.path.parent() {
            std::fs::create_dir_all(parent)?;
        }
        std::fs::write(&self.path, json)?;
        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::path::PathBuf;

    fn temp_path(name: &str) -> PathBuf {
        std::env::temp_dir().join(format!("rill_test_{name}_{}", std::process::id()))
    }

    #[tokio::test]
    async fn put_get_roundtrip() {
        let path = temp_path("put_get");
        let _ = std::fs::remove_file(&path);

        let backend = LocalBackend::new(path.clone(), None).unwrap();
        backend.put(b"hello", b"world").await.unwrap();
        let val = backend.get(b"hello").await.unwrap();
        assert_eq!(val, Some(b"world".to_vec()));

        let _ = std::fs::remove_file(&path);
    }

    #[tokio::test]
    async fn checkpoint_and_restore() {
        let path = temp_path("checkpoint");
        let _ = std::fs::remove_file(&path);

        {
            let backend = LocalBackend::new(path.clone(), None).unwrap();
            backend.put(b"key1", b"val1").await.unwrap();
            backend.put(b"key2", b"val2").await.unwrap();
            backend.checkpoint().await.unwrap();
        }

        // Load from disk
        let backend = LocalBackend::new(path.clone(), None).unwrap();
        assert_eq!(backend.get(b"key1").await.unwrap(), Some(b"val1".to_vec()));
        assert_eq!(backend.get(b"key2").await.unwrap(), Some(b"val2".to_vec()));

        let _ = std::fs::remove_file(&path);
    }

    #[tokio::test]
    async fn delete_removes_key() {
        let path = temp_path("delete");
        let _ = std::fs::remove_file(&path);

        let backend = LocalBackend::new(path.clone(), None).unwrap();
        backend.put(b"key", b"val").await.unwrap();
        backend.delete(b"key").await.unwrap();
        assert_eq!(backend.get(b"key").await.unwrap(), None);

        let _ = std::fs::remove_file(&path);
    }

    #[tokio::test]
    async fn simulated_latency() {
        let path = temp_path("latency");
        let _ = std::fs::remove_file(&path);

        let backend = LocalBackend::new(path.clone(), Some(Duration::from_millis(50))).unwrap();
        backend.put(b"k", b"v").await.unwrap();

        let start = std::time::Instant::now();
        let _ = backend.get(b"k").await.unwrap();
        let elapsed = start.elapsed();
        assert!(
            elapsed >= Duration::from_millis(40),
            "expected simulated latency"
        );

        let _ = std::fs::remove_file(&path);
    }
}
