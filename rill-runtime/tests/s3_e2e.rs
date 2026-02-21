#![cfg(feature = "s3")]

//! End-to-end S3 tiered storage test.
//!
//! Exercises the full L1 memtable → L2 Foyer → L3 SlateDB/S3 storage path
//! against a real S3-compatible service (`MinIO`).
//!
//! Pipeline topology:
//!
//! ```text
//! VecSource(words) → key_by(first_char) → WordCounter → CollectSink
//! ```

use std::collections::HashMap;
use std::sync::{Arc, Mutex};

use async_trait::async_trait;
use futures::TryStreamExt;
use object_store::ObjectStore;
use object_store::aws::AmazonS3Builder;
use rill_core::connectors::vec_source::VecSource;
use rill_core::state::backend::StateBackend;
use rill_core::state::context::StateContext;
use rill_core::state::slatedb_backend::SlateDbBackend;
use rill_core::state::tiered_backend::TieredBackendConfig;
use rill_core::traits::{Sink, StreamFunction};
use rill_runtime::dataflow::DataflowGraph;
use rill_runtime::executor::Executor;

// ── Helpers ────────────────────────────────────────────────────────

fn env_or(var: &str, default: &str) -> String {
    std::env::var(var).unwrap_or_else(|_| default.to_string())
}

fn unique_path(prefix: &str) -> String {
    format!(
        "{prefix}_{}_{}",
        std::process::id(),
        std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .unwrap()
            .as_millis()
    )
}

fn build_s3_store() -> Arc<dyn ObjectStore> {
    let s3 = AmazonS3Builder::new()
        .with_endpoint(env_or("S3_ENDPOINT", "http://localhost:9000"))
        .with_bucket_name(env_or("S3_BUCKET", "rill-test"))
        .with_access_key_id(env_or("S3_ACCESS_KEY", "minioadmin"))
        .with_secret_access_key(env_or("S3_SECRET_KEY", "minioadmin"))
        .with_region(env_or("S3_REGION", "us-east-1"))
        .with_allow_http(true)
        .build()
        .expect("failed to build S3 object store");
    Arc::new(s3)
}

// ── Domain types ───────────────────────────────────────────────────

struct CollectSink<T> {
    collected: Arc<Mutex<Vec<T>>>,
}

#[async_trait]
impl<T: Send + Sync + 'static> Sink for CollectSink<T> {
    type Input = T;

    async fn write(&mut self, input: T) -> anyhow::Result<()> {
        self.collected.lock().unwrap().push(input);
        Ok(())
    }
}

#[derive(Clone)]
struct WordCounter;

#[async_trait]
impl StreamFunction for WordCounter {
    type Input = String;
    type Output = String;

    async fn process(
        &mut self,
        input: String,
        ctx: &mut StateContext,
    ) -> anyhow::Result<Vec<String>> {
        let word = input.trim();
        let key = word.as_bytes();
        let count = match ctx.get(key).await? {
            Some(bytes) => {
                let n = u64::from_le_bytes(bytes.try_into().unwrap_or([0; 8]));
                n + 1
            }
            None => 1,
        };
        ctx.put(key, &count.to_le_bytes());
        Ok(vec![format!("{word}:{count}")])
    }
}

// ── Word generation ────────────────────────────────────────────────

fn generate_words() -> Vec<String> {
    let base_words = [
        "alpha", "arden", "arrow", "azure", "apex", "beta", "blaze", "brook", "bright", "brine",
        "cedar", "cliff", "coral", "crest", "crane", "delta", "drift", "dusk", "dawn", "depth",
        "ember", "echo", "edge", "elm", "earth", "frost", "fern", "flame", "field", "forge",
        "gale", "grove", "gleam", "grain", "gate", "haze", "helm", "haven", "husk", "hilt", "iris",
        "isle", "iron", "ivory", "inlet", "jade", "jest", "jetty", "jewel", "junco", "knoll",
        "kelp", "kern", "kite", "knot", "lark", "leaf", "lodge", "lumen", "lyric",
    ];
    let mut words = Vec::new();
    for &w in &base_words {
        for _ in 0..5 {
            words.push(w.to_string());
        }
    }
    words
}

fn expected_counts(words: &[String]) -> HashMap<String, u64> {
    let mut counts = HashMap::new();
    for w in words {
        *counts.entry(w.clone()).or_insert(0u64) += 1;
    }
    counts
}

// ── Test ────────────────────────────────────────────────────────────

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
#[allow(clippy::too_many_lines)]
async fn s3_tiered_storage_e2e() {
    let _ = tracing_subscriber::fmt()
        .with_env_filter("info")
        .with_test_writer()
        .try_init();

    let s3_store = build_s3_store();
    let slate_path = unique_path("s3_e2e");

    // ── 1. Open SlateDB on S3 ──────────────────────────────────────
    let l3 = Arc::new(
        SlateDbBackend::open(slate_path.as_str(), s3_store.clone())
            .await
            .expect("failed to open SlateDB on S3"),
    );

    // ── 2. Configure tiered storage ────────────────────────────────
    let foyer_dir = tempfile::tempdir().unwrap();
    let checkpoint_dir = tempfile::tempdir().unwrap();

    let foyer_config = TieredBackendConfig {
        foyer_dir: foyer_dir.path().to_path_buf(),
        foyer_memory_capacity: 4 * 1024 * 1024,
        foyer_disk_capacity: 16 * 1024 * 1024,
    };

    let executor = Executor::new(checkpoint_dir.path().to_path_buf()).with_tiered_storage(
        checkpoint_dir.path().to_path_buf(),
        l3.clone(),
        foyer_config,
    );

    // ── 3. Build pipeline ──────────────────────────────────────────
    let words = generate_words();
    let expected = expected_counts(&words);
    let collected = Arc::new(Mutex::new(Vec::<String>::new()));

    let graph = DataflowGraph::new();
    let stream = graph.source(VecSource::new(words));
    stream
        .key_by(|w: &String| w.chars().next().unwrap_or('_').to_string())
        .operator("word_counter", WordCounter)
        .sink(CollectSink {
            collected: collected.clone(),
        });

    // ── 4. Run pipeline (single worker — checkpoints at completion)
    executor
        .run(graph)
        .await
        .expect("pipeline execution failed");

    // ── 5. Verify output ───────────────────────────────────────────
    let results = collected.lock().unwrap().clone();
    assert!(!results.is_empty(), "pipeline produced no output");

    // Parse "word:count" outputs and find the final (max) count per word.
    let mut final_counts: HashMap<String, u64> = HashMap::new();
    for r in &results {
        let (word, count_str) = r
            .rsplit_once(':')
            .unwrap_or_else(|| panic!("unexpected output format: {r}"));
        let count: u64 = count_str.parse().expect("bad count");
        let entry = final_counts.entry(word.to_string()).or_insert(0);
        *entry = (*entry).max(count);
    }

    for (word, expected_count) in &expected {
        let actual = final_counts.get(word).copied().unwrap_or(0);
        assert_eq!(
            actual, *expected_count,
            "count mismatch for {word}: actual={actual}, expected={expected_count}"
        );
    }

    eprintln!(
        "Output verified: {} results, {} distinct words",
        results.len(),
        final_counts.len()
    );

    // ── 6. Close L3 to flush pending writes ────────────────────────
    l3.close().await.expect("failed to close SlateDB");

    // ── 7. Verify S3 persistence ───────────────────────────────────
    // Reopen a fresh SlateDB at the same S3 path and read back state keys.
    // Keys in SlateDB are prefixed: "word_counter_w0/{word}" (operator_name_w0/).
    let l3_verify = SlateDbBackend::open(slate_path.as_str(), s3_store.clone())
        .await
        .expect("failed to reopen SlateDB on S3");

    let prefix = b"word_counter_w0/";
    let verify_words = ["alpha", "beta", "cedar", "delta", "ember", "frost"];
    for word in &verify_words {
        let mut key = prefix.to_vec();
        key.extend_from_slice(word.as_bytes());

        let val = l3_verify
            .get(&key)
            .await
            .unwrap_or_else(|e| panic!("failed to read key for {word}: {e}"));

        assert!(
            val.is_some(),
            "key for '{word}' not found in S3 — state not persisted"
        );

        let bytes = val.unwrap();
        let count = u64::from_le_bytes(bytes.try_into().expect("persisted value has wrong length"));
        assert_eq!(
            count, 5,
            "persisted count for '{word}': actual={count}, expected=5"
        );
    }

    eprintln!("S3 persistence verified for {} words", verify_words.len());
    l3_verify.close().await.expect("failed to close verify DB");

    // ── 8. Verify S3 objects exist ─────────────────────────────────
    let obj_prefix = object_store::path::Path::from(slate_path.clone());
    let objects: Vec<_> = s3_store
        .list(Some(&obj_prefix))
        .try_collect()
        .await
        .expect("failed to list S3 objects");

    assert!(
        !objects.is_empty(),
        "no objects found in S3 under '{slate_path}' — SlateDB did not write to S3"
    );

    eprintln!(
        "S3 objects verified: {} objects under '{slate_path}'",
        objects.len()
    );
}
