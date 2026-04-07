#![allow(clippy::unwrap_used, clippy::expect_used)]
//! End-to-end tests for checkpoint restart with manifest persistence.

use std::collections::HashMap;
use std::sync::{Arc, Mutex};

use async_trait::async_trait;
use rhei_core::checkpoint::CheckpointManifest;
use rhei_core::connectors::partitioned_vec_source::PartitionedVecSource;
use rhei_core::connectors::vec_source::VecSource;
use rhei_core::state::context::StateContext;
use rhei_core::traits::{Sink, Source, StreamFunction};
use rhei_runtime::dataflow::DataflowGraph;
use rhei_runtime::executor::Executor;

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
        let count = ctx.get::<u64>(key).await?.unwrap_or(0) + 1;
        ctx.put(key, &count)?;
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
    let dir = std::env::temp_dir().join(format!("rhei_ckpt_restart_{}", std::process::id()));
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
    let dir = std::env::temp_dir().join(format!("rhei_ckpt_mismatch_{}", std::process::id()));
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

// ── Offset-tracking partitioned source for E2E tests ────────────

/// A partitioned source factory whose partition readers track per-partition
/// offsets. Mirrors Kafka's offset model: each partition has an independent
/// offset that can be saved to the checkpoint manifest and restored on restart.
struct OffsetPartitionedSource {
    partitions: Vec<Vec<String>>,
}

impl OffsetPartitionedSource {
    fn new(partitions: Vec<Vec<String>>) -> Self {
        Self { partitions }
    }
}

#[async_trait]
impl Source for OffsetPartitionedSource {
    type Output = String;

    async fn next_batch(&mut self) -> Option<Vec<String>> {
        None // Factory only — never called directly.
    }

    fn partition_count(&self) -> Option<usize> {
        Some(self.partitions.len())
    }

    fn create_partition_source(
        &self,
        assigned: &[usize],
    ) -> Option<Box<dyn Source<Output = String>>> {
        let partition_data: Vec<(usize, Vec<String>)> = assigned
            .iter()
            .map(|&idx| (idx, self.partitions[idx].clone()))
            .collect();
        Some(Box::new(OffsetPartitionReader {
            partitions: partition_data,
            positions: HashMap::new(),
            current_partition: 0,
        }))
    }
}

/// A partition reader that round-robins through its assigned partitions,
/// tracking per-partition offsets for checkpoint persistence and restore.
struct OffsetPartitionReader {
    partitions: Vec<(usize, Vec<String>)>,
    positions: HashMap<usize, usize>,
    current_partition: usize,
}

#[async_trait]
impl Source for OffsetPartitionReader {
    type Output = String;

    async fn next_batch(&mut self) -> Option<Vec<String>> {
        let n = self.partitions.len();
        for _ in 0..n {
            let idx = self.current_partition % n;
            self.current_partition += 1;
            let (pid, ref items) = self.partitions[idx];
            let pos = self.positions.get(&pid).copied().unwrap_or(0);
            if pos < items.len() {
                self.positions.insert(pid, pos + 1);
                return Some(vec![items[pos].clone()]);
            }
        }
        None
    }

    fn current_offsets(&self) -> HashMap<String, String> {
        self.positions
            .iter()
            .filter(|&(_, pos)| *pos > 0)
            .map(|(&pid, &pos)| (format!("src/{pid}"), (pos - 1).to_string()))
            .collect()
    }

    async fn restore_offsets(&mut self, offsets: &HashMap<String, String>) -> anyhow::Result<()> {
        for &(pid, _) in &self.partitions {
            if let Some(val) = offsets.get(&format!("src/{pid}")) {
                self.positions.insert(pid, val.parse::<usize>()? + 1);
            }
        }
        Ok(())
    }
}

// ── Multi-worker checkpoint restart ─────────────────────────────

#[tokio::test]
async fn multi_worker_checkpoint_restart() {
    let dir = std::env::temp_dir().join(format!("rhei_ckpt_multi_{}", std::process::id()));
    let _ = std::fs::remove_dir_all(&dir);
    std::fs::create_dir_all(&dir).unwrap();

    // ── Run 1: 2 workers, process ["a","b","c","a","b"] ──────────
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

        graph
            .source(source)
            .key_by(|w: &String| w.clone())
            .operator("word_counter", WordCounter)
            .sink(CollectSink {
                collected: collected.clone(),
            });

        let executor = Executor::builder()
            .checkpoint_dir(&dir)
            .workers(2)
            .build()
            .unwrap();
        executor.run(graph).await.unwrap();

        let results = collected.lock().unwrap().clone();
        let counts = parse_final_counts(&results);
        assert_eq!(counts.get("a").copied(), Some(2), "run 1: a should be 2");
        assert_eq!(counts.get("b").copied(), Some(2), "run 1: b should be 2");
        assert_eq!(counts.get("c").copied(), Some(1), "run 1: c should be 1");
    }

    let manifest1 = CheckpointManifest::load(&dir).expect("manifest after run 1");
    assert!(manifest1.checkpoint_id >= 1);

    // ── Run 2: same dir, new data, 2 workers ────────────────────
    {
        let graph = DataflowGraph::new();
        let source = VecSource::new(vec!["a".to_string(), "c".to_string(), "d".to_string()]);
        let collected = Arc::new(Mutex::new(Vec::new()));

        graph
            .source(source)
            .key_by(|w: &String| w.clone())
            .operator("word_counter", WordCounter)
            .sink(CollectSink {
                collected: collected.clone(),
            });

        let executor = Executor::builder()
            .checkpoint_dir(&dir)
            .workers(2)
            .build()
            .unwrap();
        executor.run(graph).await.unwrap();

        let results = collected.lock().unwrap().clone();
        let counts = parse_final_counts(&results);
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

    let manifest2 = CheckpointManifest::load(&dir).expect("manifest after run 2");
    assert!(
        manifest2.checkpoint_id > manifest1.checkpoint_id,
        "checkpoint_id should have incremented"
    );

    let _ = std::fs::remove_dir_all(&dir);
}

// ── Partitioned source checkpoint restart ────────────────────────

#[tokio::test]
async fn partitioned_source_checkpoint_restart() {
    let dir = std::env::temp_dir().join(format!("rhei_ckpt_partsrc_{}", std::process::id()));
    let _ = std::fs::remove_dir_all(&dir);
    std::fs::create_dir_all(&dir).unwrap();

    // ── Run 1: PartitionedVecSource, 4 partitions, 2 workers ────
    {
        let graph = DataflowGraph::new();
        let source = PartitionedVecSource::new(
            vec![
                "a".to_string(),
                "b".to_string(),
                "c".to_string(),
                "a".to_string(),
                "b".to_string(),
            ],
            4,
        );
        let collected = Arc::new(Mutex::new(Vec::new()));

        graph
            .source(source)
            .key_by(|w: &String| w.clone())
            .operator("word_counter", WordCounter)
            .sink(CollectSink {
                collected: collected.clone(),
            });

        let executor = Executor::builder()
            .checkpoint_dir(&dir)
            .workers(2)
            .build()
            .unwrap();
        executor.run(graph).await.unwrap();

        let results = collected.lock().unwrap().clone();
        let counts = parse_final_counts(&results);
        assert_eq!(counts.get("a").copied(), Some(2), "run 1: a should be 2");
        assert_eq!(counts.get("b").copied(), Some(2), "run 1: b should be 2");
        assert_eq!(counts.get("c").copied(), Some(1), "run 1: c should be 1");
    }

    let manifest1 = CheckpointManifest::load(&dir).expect("manifest after run 1");
    assert!(manifest1.checkpoint_id >= 1);

    // ── Run 2: new data through partitioned source ──────────────
    {
        let graph = DataflowGraph::new();
        let source =
            PartitionedVecSource::new(vec!["a".to_string(), "c".to_string(), "d".to_string()], 4);
        let collected = Arc::new(Mutex::new(Vec::new()));

        graph
            .source(source)
            .key_by(|w: &String| w.clone())
            .operator("word_counter", WordCounter)
            .sink(CollectSink {
                collected: collected.clone(),
            });

        let executor = Executor::builder()
            .checkpoint_dir(&dir)
            .workers(2)
            .build()
            .unwrap();
        executor.run(graph).await.unwrap();

        let results = collected.lock().unwrap().clone();
        let counts = parse_final_counts(&results);
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

    let manifest2 = CheckpointManifest::load(&dir).expect("manifest after run 2");
    assert!(
        manifest2.checkpoint_id > manifest1.checkpoint_id,
        "checkpoint_id should have incremented"
    );

    let _ = std::fs::remove_dir_all(&dir);
}

// ── Offset merge: manifest contains merged per-partition offsets ─

#[tokio::test]
async fn partitioned_source_offset_merge() {
    let dir = std::env::temp_dir().join(format!("rhei_ckpt_offmerge_{}", std::process::id()));
    let _ = std::fs::remove_dir_all(&dir);
    std::fs::create_dir_all(&dir).unwrap();

    let source = OffsetPartitionedSource::new(vec![
        vec!["p0_a".into(), "p0_b".into(), "p0_c".into()],
        vec!["p1_a".into(), "p1_b".into(), "p1_c".into()],
        vec!["p2_a".into(), "p2_b".into(), "p2_c".into()],
        vec!["p3_a".into(), "p3_b".into(), "p3_c".into()],
    ]);

    let graph = DataflowGraph::new();
    let collected = Arc::new(Mutex::new(Vec::new()));

    graph
        .source(source)
        .key_by(|w: &String| w.clone())
        .operator("counter", WordCounter)
        .sink(CollectSink {
            collected: collected.clone(),
        });

    let executor = Executor::builder()
        .checkpoint_dir(&dir)
        .workers(2)
        .build()
        .unwrap();
    executor.run(graph).await.unwrap();

    let results = collected.lock().unwrap().clone();
    assert_eq!(results.len(), 12, "4 partitions * 3 items = 12 outputs");

    // Verify manifest contains per-partition offsets from both workers.
    // With 4 partitions and 2 workers (round-robin):
    //   Worker 0 reader: partitions [0, 2] → offsets "src/0", "src/2"
    //   Worker 1 reader: partitions [1, 3] → offsets "src/1", "src/3"
    // After merge: 4 offset entries in the manifest.
    let manifest = CheckpointManifest::load(&dir).expect("manifest should exist");
    assert_eq!(
        manifest.source_offsets.len(),
        4,
        "should have offsets from 4 partitions, got: {:?}",
        manifest.source_offsets
    );
    for i in 0..4 {
        let key = format!("src/{i}");
        assert!(
            manifest.source_offsets.contains_key(&key),
            "missing offset for partition {i}"
        );
        // Each partition had 3 items → last offset = 2 (0-indexed).
        assert_eq!(
            manifest.source_offsets[&key], "2",
            "partition {i} offset should be 2"
        );
    }

    let _ = std::fs::remove_dir_all(&dir);
}

// ── Offset restore: second run skips already-processed items ────

#[tokio::test]
#[allow(clippy::too_many_lines)]
async fn partitioned_source_offset_restore() {
    let dir = std::env::temp_dir().join(format!("rhei_ckpt_offrestore_{}", std::process::id()));
    let _ = std::fs::remove_dir_all(&dir);
    std::fs::create_dir_all(&dir).unwrap();

    // ── Run 1: 4 partitions, 3 items each ────────────────────────
    {
        let source = OffsetPartitionedSource::new(vec![
            vec!["p0_a".into(), "p0_b".into(), "p0_c".into()],
            vec!["p1_a".into(), "p1_b".into(), "p1_c".into()],
            vec!["p2_a".into(), "p2_b".into(), "p2_c".into()],
            vec!["p3_a".into(), "p3_b".into(), "p3_c".into()],
        ]);

        let graph = DataflowGraph::new();
        let collected = Arc::new(Mutex::new(Vec::new()));

        graph
            .source(source)
            .key_by(|w: &String| w.clone())
            .operator("counter", WordCounter)
            .sink(CollectSink {
                collected: collected.clone(),
            });

        let executor = Executor::builder()
            .checkpoint_dir(&dir)
            .workers(2)
            .build()
            .unwrap();
        executor.run(graph).await.unwrap();

        let results = collected.lock().unwrap().clone();
        assert_eq!(results.len(), 12);
        let counts = parse_final_counts(&results);
        // Every word appears exactly once across all partitions.
        for (word, count) in &counts {
            assert_eq!(*count, 1, "{word} should appear exactly once in run 1");
        }
    }

    // ── Run 2: same partitions + 2 new items each (5 total) ─────
    // Offsets from run 1 are restored, so each reader skips the
    // first 3 items per partition and only processes new items.
    {
        let source = OffsetPartitionedSource::new(vec![
            vec![
                "p0_a".into(),
                "p0_b".into(),
                "p0_c".into(),
                "p0_d".into(),
                "p0_e".into(),
            ],
            vec![
                "p1_a".into(),
                "p1_b".into(),
                "p1_c".into(),
                "p1_d".into(),
                "p1_e".into(),
            ],
            vec![
                "p2_a".into(),
                "p2_b".into(),
                "p2_c".into(),
                "p2_d".into(),
                "p2_e".into(),
            ],
            vec![
                "p3_a".into(),
                "p3_b".into(),
                "p3_c".into(),
                "p3_d".into(),
                "p3_e".into(),
            ],
        ]);

        let graph = DataflowGraph::new();
        let collected = Arc::new(Mutex::new(Vec::new()));

        graph
            .source(source)
            .key_by(|w: &String| w.clone())
            .operator("counter", WordCounter)
            .sink(CollectSink {
                collected: collected.clone(),
            });

        let executor = Executor::builder()
            .checkpoint_dir(&dir)
            .workers(2)
            .build()
            .unwrap();
        executor.run(graph).await.unwrap();

        let results = collected.lock().unwrap().clone();
        let counts = parse_final_counts(&results);

        // New items (p*_d, p*_e) should be processed: 4 partitions * 2 = 8 outputs.
        assert_eq!(
            results.len(),
            8,
            "only new items should be processed after offset restore"
        );

        // Old items (p*_a, p*_b, p*_c) should NOT have been reprocessed.
        // Their operator state from run 1 persists (count = 1), but no new
        // increment means they don't appear in run 2 output at all.
        for (word, count) in &counts {
            assert_eq!(
                *count, 1,
                "{word} should be 1 — new items counted against restored state"
            );
            assert!(
                word.ends_with("_d") || word.ends_with("_e"),
                "only new items (_d, _e) should appear in run 2 output, got: {word}"
            );
        }
    }

    // Verify manifest offsets advanced to include new items.
    let manifest = CheckpointManifest::load(&dir).expect("manifest after run 2");
    for i in 0..4 {
        let key = format!("src/{i}");
        assert_eq!(
            manifest.source_offsets[&key], "4",
            "partition {i} offset should be 4 after processing 5 items total"
        );
    }

    let _ = std::fs::remove_dir_all(&dir);
}

// ── Worker count change between restarts ────────────────────────

#[tokio::test]
#[allow(clippy::too_many_lines)]
async fn partitioned_source_changed_worker_count() {
    // Run 1: 2 workers, 4 partitions.
    //   Worker 0: partitions [0, 2], Worker 1: partitions [1, 3]
    // Run 2: 4 workers, same 4 partitions.
    //   Worker 0: [0], Worker 1: [1], Worker 2: [2], Worker 3: [3]
    //
    // Per-partition offsets (src/0, src/1, src/2, src/3) are keyed by
    // partition ID, so restore works regardless of worker distribution.
    let dir =
        std::env::temp_dir().join(format!("rhei_ckpt_changed_workers_{}", std::process::id()));
    let _ = std::fs::remove_dir_all(&dir);
    std::fs::create_dir_all(&dir).unwrap();

    // ── Run 1: 2 workers, 4 partitions, 3 items each ────────────
    {
        let source = OffsetPartitionedSource::new(vec![
            vec!["p0_a".into(), "p0_b".into(), "p0_c".into()],
            vec!["p1_a".into(), "p1_b".into(), "p1_c".into()],
            vec!["p2_a".into(), "p2_b".into(), "p2_c".into()],
            vec!["p3_a".into(), "p3_b".into(), "p3_c".into()],
        ]);

        let graph = DataflowGraph::new();
        let collected = Arc::new(Mutex::new(Vec::new()));

        graph
            .source(source)
            .key_by(|w: &String| w.clone())
            .operator("counter", WordCounter)
            .sink(CollectSink {
                collected: collected.clone(),
            });

        let executor = Executor::builder()
            .checkpoint_dir(&dir)
            .workers(2)
            .build()
            .unwrap();
        executor.run(graph).await.unwrap();

        let results = collected.lock().unwrap().clone();
        assert_eq!(results.len(), 12);
    }

    let manifest1 = CheckpointManifest::load(&dir).expect("manifest after run 1");
    assert_eq!(manifest1.source_offsets.len(), 4);

    // ── Run 2: 4 workers (changed!), same partitions + 2 new items ─
    {
        let source = OffsetPartitionedSource::new(vec![
            vec![
                "p0_a".into(),
                "p0_b".into(),
                "p0_c".into(),
                "p0_d".into(),
                "p0_e".into(),
            ],
            vec![
                "p1_a".into(),
                "p1_b".into(),
                "p1_c".into(),
                "p1_d".into(),
                "p1_e".into(),
            ],
            vec![
                "p2_a".into(),
                "p2_b".into(),
                "p2_c".into(),
                "p2_d".into(),
                "p2_e".into(),
            ],
            vec![
                "p3_a".into(),
                "p3_b".into(),
                "p3_c".into(),
                "p3_d".into(),
                "p3_e".into(),
            ],
        ]);

        let graph = DataflowGraph::new();
        let collected = Arc::new(Mutex::new(Vec::new()));

        graph
            .source(source)
            .key_by(|w: &String| w.clone())
            .operator("counter", WordCounter)
            .sink(CollectSink {
                collected: collected.clone(),
            });

        // Changed from 2 workers to 4 workers — partition distribution changes
        // but per-partition offsets should still restore correctly.
        let executor = Executor::builder()
            .checkpoint_dir(&dir)
            .workers(4)
            .build()
            .unwrap();
        executor.run(graph).await.unwrap();

        let results = collected.lock().unwrap().clone();
        let counts = parse_final_counts(&results);

        // Only new items should be processed (offset restore skips old items).
        assert_eq!(
            results.len(),
            8,
            "only 8 new items should be processed after offset restore with changed workers"
        );

        for (word, count) in &counts {
            assert!(
                word.ends_with("_d") || word.ends_with("_e"),
                "only new items should appear in output, got: {word}"
            );
            assert_eq!(
                *count, 1,
                "{word} should be counted once against restored state"
            );
        }
    }

    let _ = std::fs::remove_dir_all(&dir);
}
