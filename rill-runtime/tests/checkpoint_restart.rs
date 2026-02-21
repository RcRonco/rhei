//! End-to-end tests for checkpoint restart with manifest persistence.

use std::collections::HashMap;
use std::sync::{Arc, Mutex};

use async_trait::async_trait;
use rill_core::checkpoint::CheckpointManifest;
use rill_core::connectors::vec_source::VecSource;
use rill_core::state::context::StateContext;
use rill_core::traits::{Sink, StreamFunction};
use rill_runtime::dataflow::DataflowGraph;
use rill_runtime::executor::Executor;

// ── Helpers ────────────────────────────────────────────────────────

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

/// Parse "word:count" output into a map of final (max) counts per word.
fn parse_final_counts(results: &[String]) -> HashMap<String, u64> {
    let mut counts: HashMap<String, u64> = HashMap::new();
    for r in results {
        let (word, count_str) = r
            .rsplit_once(':')
            .unwrap_or_else(|| panic!("unexpected output format: {r}"));
        let count: u64 = count_str.parse().expect("bad count");
        let entry = counts.entry(word.to_string()).or_insert(0);
        *entry = (*entry).max(count);
    }
    counts
}

// ── Tests ──────────────────────────────────────────────────────────

#[tokio::test]
async fn checkpoint_restart_preserves_state() {
    let dir = std::env::temp_dir().join(format!("rill_ckpt_restart_{}", std::process::id()));
    let _ = std::fs::remove_dir_all(&dir);
    std::fs::create_dir_all(&dir).unwrap();

    // ── Run 1: process ["a","b","c","a","b"] ──────────────────────
    {
        let graph = DataflowGraph::new();
        let source = VecSource::new(vec![
            "a".to_string(),
            "b".to_string(),
            "c".to_string(),
            "a".to_string(),
            "b".to_string(),
        ]);
        let collected = Arc::new(Mutex::new(Vec::new()));

        let stream = graph.source(source);
        stream
            .key_by(|w: &String| w.clone())
            .operator("word_counter", WordCounter)
            .sink(CollectSink {
                collected: collected.clone(),
            });

        let executor = Executor::new(dir.clone());
        executor.run(graph).await.unwrap();

        let results = collected.lock().unwrap().clone();
        let counts = parse_final_counts(&results);
        assert_eq!(counts.get("a").copied(), Some(2), "run 1: a should be 2");
        assert_eq!(counts.get("b").copied(), Some(2), "run 1: b should be 2");
        assert_eq!(counts.get("c").copied(), Some(1), "run 1: c should be 1");
    }

    // Verify manifest exists after run 1.
    let manifest1 = CheckpointManifest::load(&dir).expect("manifest should exist after run 1");
    assert_eq!(manifest1.version, 1);
    assert!(manifest1.checkpoint_id >= 1);
    assert!(manifest1.operators.contains(&"word_counter".to_string()));

    // ── Run 2: process ["a","c","d"] with same checkpoint dir ─────
    {
        let graph = DataflowGraph::new();
        let source = VecSource::new(vec!["a".to_string(), "c".to_string(), "d".to_string()]);
        let collected = Arc::new(Mutex::new(Vec::new()));

        let stream = graph.source(source);
        stream
            .key_by(|w: &String| w.clone())
            .operator("word_counter", WordCounter)
            .sink(CollectSink {
                collected: collected.clone(),
            });

        let executor = Executor::new(dir.clone());
        executor.run(graph).await.unwrap();

        let results = collected.lock().unwrap().clone();
        let counts = parse_final_counts(&results);
        // Counts should be cumulative from run 1.
        assert_eq!(
            counts.get("a").copied(),
            Some(3),
            "run 2: a should be 2+1=3"
        );
        assert_eq!(
            counts.get("c").copied(),
            Some(2),
            "run 2: c should be 1+1=2"
        );
        assert_eq!(
            counts.get("d").copied(),
            Some(1),
            "run 2: d should be 1 (new)"
        );
    }

    // Verify manifest was updated.
    let manifest2 = CheckpointManifest::load(&dir).expect("manifest should exist after run 2");
    assert!(
        manifest2.checkpoint_id > manifest1.checkpoint_id,
        "checkpoint_id should have incremented: {} > {}",
        manifest2.checkpoint_id,
        manifest1.checkpoint_id,
    );

    let _ = std::fs::remove_dir_all(&dir);
}

#[tokio::test]
async fn checkpoint_manifest_detects_operator_mismatch() {
    let dir = std::env::temp_dir().join(format!("rill_ckpt_mismatch_{}", std::process::id()));
    let _ = std::fs::remove_dir_all(&dir);
    std::fs::create_dir_all(&dir).unwrap();

    // ── Run 1: operator named "counter_v1" ────────────────────────
    {
        let graph = DataflowGraph::new();
        let source = VecSource::new(vec!["x".to_string()]);
        let collected = Arc::new(Mutex::new(Vec::new()));

        let stream = graph.source(source);
        stream
            .key_by(|w: &String| w.clone())
            .operator("counter_v1", WordCounter)
            .sink(CollectSink {
                collected: collected.clone(),
            });

        let executor = Executor::new(dir.clone());
        executor.run(graph).await.unwrap();

        let results = collected.lock().unwrap().clone();
        assert_eq!(results, vec!["x:1"]);
    }

    let manifest1 = CheckpointManifest::load(&dir).expect("manifest should exist");
    assert!(manifest1.operators.contains(&"counter_v1".to_string()));

    // ── Run 2: different operator name "counter_v2" ───────────────
    // This should log a warning about operator mismatch but still run.
    {
        let graph = DataflowGraph::new();
        let source = VecSource::new(vec!["y".to_string()]);
        let collected = Arc::new(Mutex::new(Vec::new()));

        let stream = graph.source(source);
        stream
            .key_by(|w: &String| w.clone())
            .operator("counter_v2", WordCounter)
            .sink(CollectSink {
                collected: collected.clone(),
            });

        let executor = Executor::new(dir.clone());
        executor.run(graph).await.unwrap();

        // New operator starts with empty state, so count should be 1.
        let results = collected.lock().unwrap().clone();
        assert_eq!(results, vec!["y:1"]);
    }

    let manifest2 = CheckpointManifest::load(&dir).expect("manifest should exist after run 2");
    assert!(manifest2.operators.contains(&"counter_v2".to_string()));
    assert!(!manifest2.operators.contains(&"counter_v1".to_string()));

    let _ = std::fs::remove_dir_all(&dir);
}
