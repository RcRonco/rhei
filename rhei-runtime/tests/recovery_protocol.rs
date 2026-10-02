#![allow(clippy::unwrap_used, clippy::expect_used)]
//! Checkpoints bind admitted input, all workers' state, and durable sink output.

use async_trait::async_trait;
use rhei_core::arrow::{
    BufferOutput, OperatorContext, RheiBuffer, RheiBuilder, RheiSchema, Sink, StreamFunction,
};
use rhei_core::connectors::batch::VecSource;
use rhei_macros::RheiSchema;
use rhei_runtime::controller::PipelineController;
use rhei_runtime::dataflow::DataflowGraph;
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::{Arc, Mutex};
use std::time::Duration;

#[derive(Clone, Debug, RheiSchema)]
struct Record {
    key: String,
    value: u64,
}

#[derive(Clone)]
struct Counter;

#[async_trait]
impl StreamFunction for Counter {
    type Input = Record;
    type Output = Record;
    async fn process(
        &mut self,
        input: RheiBuffer<Record>,
        ctx: &mut OperatorContext,
    ) -> anyhow::Result<BufferOutput<Record>> {
        let records: Vec<_> = input.iter().map(|r| (r.key.to_owned(), r.value)).collect();
        let mut out = Record::builder(records.len());
        for (key, _) in records {
            let count: u64 = ctx
                .state
                .get_keyed(key.as_bytes(), b"count")
                .await?
                .unwrap_or(0);
            ctx.state
                .put_keyed(key.as_bytes(), b"count", &(count + 1))?;
            out.append(Record {
                key,
                value: count + 1,
            });
        }
        Ok(BufferOutput::Single(RheiBuffer::from_builder(out)))
    }
}

struct Output {
    durable: Arc<Mutex<Vec<(String, u64)>>>,
    pending: Vec<(String, u64)>,
    fail_flush: Arc<AtomicBool>,
    flushes: Arc<std::sync::atomic::AtomicUsize>,
}

#[async_trait]
impl Sink for Output {
    type Input = Record;
    async fn write_batch(&mut self, input: RheiBuffer<Record>) -> anyhow::Result<()> {
        self.pending
            .extend(input.iter().map(|r| (r.key.to_owned(), r.value)));
        Ok(())
    }
    async fn flush(&mut self) -> anyhow::Result<()> {
        let call = self.flushes.fetch_add(1, Ordering::SeqCst);
        anyhow::ensure!(
            !(self.fail_flush.load(Ordering::SeqCst) && call == 1),
            "injected sink flush failure"
        );
        self.durable.lock().unwrap().append(&mut self.pending);
        Ok(())
    }
}

fn graph(output: Output) -> DataflowGraph {
    let graph = DataflowGraph::new();
    graph
        .source(
            VecSource::new(
                (0..24)
                    .map(|i| Record {
                        key: format!("key-{}", i % 3),
                        value: i,
                    })
                    .collect(),
            )
            .with_batch_size(1),
        )
        .key_by(|row| row.key.to_owned())
        .operator("counter", Counter)
        .sink(output);
    graph
}

fn output(durable: Arc<Mutex<Vec<(String, u64)>>>, fail: bool) -> Output {
    Output {
        durable,
        pending: Vec::new(),
        fail_flush: Arc::new(AtomicBool::new(fail)),
        flushes: Arc::default(),
    }
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn failed_sink_does_not_publish_prefetched_offsets_or_uncommitted_state() {
    let dir = tempfile::tempdir().unwrap();
    let durable = Arc::new(Mutex::new(Vec::new()));
    let controller = || {
        PipelineController::builder()
            .checkpoint_dir(dir.path())
            .workers(3)
            .checkpoint_interval(3)
            .coordinated_recovery(true)
            .build()
            .unwrap()
    };
    let first = tokio::time::timeout(
        Duration::from_secs(15),
        controller().run(graph(output(durable.clone(), true))),
    )
    .await
    .unwrap();
    assert!(first.unwrap_err().to_string().contains("sink"));
    let manifest: serde_json::Value =
        serde_json::from_slice(&std::fs::read(dir.path().join("recovery.json")).unwrap()).unwrap();
    assert_eq!(manifest["epoch"], 1);
    assert_eq!(
        manifest["participants"]["0"]["sources"]["0:0"]["offsets"]["next_index"],
        "3"
    );
    assert_eq!(durable.lock().unwrap().len(), 3);

    tokio::time::timeout(
        Duration::from_secs(15),
        controller().run(graph(output(durable.clone(), false))),
    )
    .await
    .unwrap()
    .unwrap();
    let mut records = durable.lock().unwrap().clone();
    records.sort();
    let expected: Vec<_> = (0..3)
        .flat_map(|key| (1..=8).map(move |count| (format!("key-{key}"), count)))
        .collect();
    assert_eq!(records, expected);
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn missing_committed_snapshot_is_an_error_and_never_starts_empty() {
    let dir = tempfile::tempdir().unwrap();
    let controller = || {
        PipelineController::builder()
            .checkpoint_dir(dir.path())
            .workers(2)
            .checkpoint_interval(5)
            .coordinated_recovery(true)
            .build()
            .unwrap()
    };
    tokio::time::timeout(
        Duration::from_secs(15),
        controller().run(graph(output(Arc::default(), false))),
    )
    .await
    .unwrap()
    .unwrap();
    let manifest: serde_json::Value =
        serde_json::from_slice(&std::fs::read(dir.path().join("recovery.json")).unwrap()).unwrap();
    let snapshot = manifest["participants"]["0"]["local_snapshots"]["counter"]
        .as_str()
        .unwrap();
    std::fs::remove_file(dir.path().join(snapshot)).unwrap();
    assert!(
        controller()
            .run(graph(output(Arc::default(), false)))
            .await
            .is_err()
    );
}

struct FileOutput {
    file: std::fs::File,
    flushes: usize,
    crash_marker: Option<std::path::PathBuf>,
}

#[async_trait]
impl Sink for FileOutput {
    type Input = Record;
    async fn write_batch(&mut self, input: RheiBuffer<Record>) -> anyhow::Result<()> {
        use std::io::Write;
        for r in &input {
            writeln!(self.file, "{} {}", r.key, r.value)?;
        }
        Ok(())
    }
    async fn flush(&mut self) -> anyhow::Result<()> {
        self.flushes += 1;
        self.file.sync_all()?;
        if self.flushes == 2
            && let Some(marker) = &self.crash_marker
        {
            std::fs::write(marker, b"ready-to-kill")?;
            std::future::pending::<()>().await;
        }
        Ok(())
    }
}

#[test]
fn recovery_cluster_process() {
    let Ok(pid) = std::env::var("RHEI_RECOVERY_TEST_PID") else {
        return;
    };
    let pid: usize = pid.parse().unwrap();
    let dir = std::path::PathBuf::from(std::env::var("RHEI_RECOVERY_TEST_DIR").unwrap());
    let peers: Vec<String> = std::env::var("RHEI_RECOVERY_TEST_PEERS")
        .unwrap()
        .split(',')
        .map(str::to_owned)
        .collect();
    let crash = std::env::var_os("RHEI_RECOVERY_TEST_CRASH").is_some();
    let runtime = tokio::runtime::Builder::new_multi_thread()
        .worker_threads(3)
        .enable_all()
        .build()
        .unwrap();
    runtime.block_on(async {
        let graph = DataflowGraph::new();
        graph
            .source(
                rhei_core::connectors::batch::PartitionedVecSource::new(
                    (0..96)
                        .map(|i| Record {
                            key: format!("key-{}", i % 3),
                            value: i,
                        })
                        .collect(),
                    4,
                )
                .with_batch_size(1),
            )
            .key_by(|row| row.key.to_owned())
            .operator("counter", Counter)
            .sink(FileOutput {
                file: std::fs::OpenOptions::new()
                    .create(true)
                    .append(true)
                    .open(dir.join(format!("output-{pid}")))
                    .unwrap(),
                flushes: 0,
                crash_marker: (crash && pid == 1).then(|| dir.join("kill-ready")),
            });
        let controller = PipelineController::builder()
            .checkpoint_dir(&dir)
            .workers(2)
            .peers(peers)
            .process_id(pid)
            .checkpoint_interval(4)
            .coordinated_recovery(true)
            .build()
            .unwrap();
        let result = controller.run(graph).await;
        if crash {
            assert!(result.is_err(), "peer death must fail the attempt");
        } else {
            result.unwrap();
        }
    });
}

fn available_ports() -> (Vec<String>, u16) {
    // Hold all three until selected; control and Timely ports must be distinct.
    let listeners: Vec<_> = (0..3)
        .map(|_| std::net::TcpListener::bind("127.0.0.1:0").unwrap())
        .collect();
    let ports: Vec<_> = listeners
        .iter()
        .map(|l| l.local_addr().unwrap().port())
        .collect();
    (
        ports[..2]
            .iter()
            .map(|p| format!("127.0.0.1:{p}"))
            .collect(),
        ports[2],
    )
}

fn spawn_cluster(dir: &std::path::Path, crash: bool) -> Vec<std::process::Child> {
    let (peers, control_port) = available_ports();
    (0..2)
        .map(|pid| {
            let mut command = std::process::Command::new(std::env::current_exe().unwrap());
            command
                .args(["--exact", "recovery_cluster_process", "--nocapture"])
                .env("RHEI_RECOVERY_TEST_PID", pid.to_string())
                .env("RHEI_RECOVERY_TEST_DIR", dir)
                .env("RHEI_RECOVERY_TEST_PEERS", peers.join(","))
                .env("RHEI_CHECKPOINT_PORT", control_port.to_string());
            if crash {
                command.env("RHEI_RECOVERY_TEST_CRASH", "1");
            }
            command.spawn().unwrap()
        })
        .collect()
}

fn wait_children(children: &mut [std::process::Child], killed: Option<usize>) {
    let deadline = std::time::Instant::now() + Duration::from_secs(25);
    for (pid, child) in children.iter_mut().enumerate() {
        loop {
            if let Some(status) = child.try_wait().unwrap() {
                if killed != Some(pid) {
                    assert!(status.success(), "process {pid} failed: {status}");
                }
                break;
            }
            if std::time::Instant::now() > deadline {
                let _ = child.kill();
                let _ = child.wait();
                panic!("process {pid} failed to finish");
            }
            std::thread::sleep(Duration::from_millis(20));
        }
    }
}

#[test]
fn two_process_recovery_after_participant_is_killed_during_sink_flush() {
    let dir = tempfile::tempdir().unwrap();
    let mut children = spawn_cluster(dir.path(), true);
    let deadline = std::time::Instant::now() + Duration::from_secs(20);
    while !dir.path().join("kill-ready").exists() {
        if std::time::Instant::now() > deadline {
            for child in &mut children {
                let _ = child.kill();
                let _ = child.wait();
            }
            panic!("cluster never reached the second checkpoint flush");
        }
        std::thread::sleep(Duration::from_millis(20));
    }
    children[1].kill().unwrap();
    wait_children(&mut children, Some(1));
    let manifest: serde_json::Value =
        serde_json::from_slice(&std::fs::read(dir.path().join("recovery.json")).unwrap()).unwrap();
    assert_eq!(
        manifest["epoch"], 1,
        "failed second epoch must not be published"
    );
    assert_eq!(manifest["participants"].as_object().unwrap().len(), 2);
    let mut children = spawn_cluster(dir.path(), false);
    wait_children(&mut children, None);

    let mut records = std::collections::BTreeSet::new();
    for pid in 0..2 {
        let text = std::fs::read_to_string(dir.path().join(format!("output-{pid}"))).unwrap();
        for line in text.lines() {
            let (key, count) = line.split_once(' ').unwrap();
            records.insert((key.to_owned(), count.parse::<u64>().unwrap()));
        }
    }
    let expected: std::collections::BTreeSet<_> = (0..3)
        .flat_map(|key| (1..=32).map(move |count| (format!("key-{key}"), count)))
        .collect();
    assert_eq!(
        records, expected,
        "replayed output may duplicate, but state must neither skip nor double-apply committed input"
    );
}

struct WindowSource {
    cursor: u64,
    pause: bool,
}

#[async_trait]
impl rhei_core::arrow::Source for WindowSource {
    fn supports_recovery(&self) -> bool {
        true
    }
    type Output = Record;
    async fn next_batch(&mut self) -> Option<RheiBuffer<Record>> {
        if self.pause && self.cursor == 1 {
            std::future::pending::<()>().await;
        }
        if self.cursor >= 2 {
            return None;
        }
        let mut builder = Record::builder(1);
        if self.cursor == 0 {
            builder.append(Record {
                key: "window-key".into(),
                value: 3,
            });
        }
        self.cursor += 1;
        Some(RheiBuffer::from_builder(builder))
    }
    fn current_watermark(&self) -> Option<u64> {
        Some(if self.cursor <= 1 { 3 } else { 20 })
    }
    fn current_offsets(&self) -> std::collections::HashMap<String, String> {
        std::collections::HashMap::from([("cursor".into(), self.cursor.to_string())])
    }
    async fn restore_offsets(
        &mut self,
        offsets: &std::collections::HashMap<String, String>,
    ) -> anyhow::Result<()> {
        self.cursor = offsets.get("cursor").map_or(Ok(0), |v| v.parse())?;
        Ok(())
    }
}

struct ShutdownOutput {
    output: Output,
    shutdown: Option<rhei_runtime::shutdown::ShutdownTrigger>,
}

#[async_trait]
impl Sink for ShutdownOutput {
    type Input = Record;
    async fn write_batch(&mut self, input: RheiBuffer<Record>) -> anyhow::Result<()> {
        self.output.write_batch(input).await
    }
    async fn flush(&mut self) -> anyhow::Result<()> {
        self.output.flush().await?;
        if let Some(trigger) = self.shutdown.take() {
            trigger.shutdown();
        }
        Ok(())
    }
}

#[derive(Clone)]
struct TimerOutput;

#[async_trait]
impl StreamFunction for TimerOutput {
    type Input = Record;
    type Output = Record;
    async fn process(
        &mut self,
        input: RheiBuffer<Record>,
        ctx: &mut OperatorContext,
    ) -> anyhow::Result<BufferOutput<Record>> {
        for row in &input {
            ctx.state.timers().register(10, row.key.to_owned());
        }
        Ok(BufferOutput::None)
    }
    async fn on_timer(
        &mut self,
        _timestamp: u64,
        key: &str,
        _ctx: &mut OperatorContext,
    ) -> anyhow::Result<BufferOutput<Record>> {
        let mut out = Record::builder(1);
        out.append(Record {
            key: key.to_owned(),
            value: 999,
        });
        Ok(BufferOutput::Single(RheiBuffer::from_builder(out)))
    }
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn open_windows_and_timers_fire_after_restart_without_new_records_for_the_key() {
    let dir = tempfile::tempdir().unwrap();
    let collected = Arc::new(Mutex::new(Vec::new()));
    for first in [true, false] {
        let graph = DataflowGraph::new();
        let stream = graph
            .source(WindowSource {
                cursor: 0,
                pause: first,
            })
            .key_by(|row| row.key.to_owned());
        let (handle, trigger) = rhei_runtime::shutdown::ShutdownHandle::new();
        let window = rhei_core::operators::tumbling_window::TumblingWindow::new(
            10,
            |row: RecordView<'_>| row.key.to_owned(),
            |row: RecordView<'_>| row.value,
            |count: &mut u64, _: RecordView<'_>| *count += 1,
            |key: &str, _, _, count: &u64| Record {
                key: key.to_owned(),
                value: *count,
            },
        );
        stream.operator("window", window).sink(ShutdownOutput {
            output: output(collected.clone(), false),
            shutdown: Some(trigger),
        });
        stream
            .operator("timer", TimerOutput)
            .sink(output(collected.clone(), false));
        let controller = PipelineController::builder()
            .checkpoint_dir(dir.path())
            .workers(3)
            .checkpoint_interval(1)
            .coordinated_recovery(true)
            .build()
            .unwrap();
        tokio::time::timeout(
            Duration::from_secs(15),
            controller.run_with_shutdown(graph, handle),
        )
        .await
        .unwrap()
        .unwrap();
        if first {
            assert!(collected.lock().unwrap().is_empty());
        }
    }
    let mut outputs = collected.lock().unwrap().clone();
    outputs.sort();
    assert_eq!(
        outputs,
        vec![("window-key".into(), 1), ("window-key".into(), 999)]
    );
}

struct UnsupportedSource;

#[async_trait]
impl rhei_core::arrow::Source for UnsupportedSource {
    type Output = Record;
    async fn next_batch(&mut self) -> Option<RheiBuffer<Record>> {
        None
    }
}

#[derive(Clone)]
struct BrokenSnapshot;

#[async_trait]
impl StreamFunction for BrokenSnapshot {
    type Input = Record;
    type Output = Record;
    async fn process(
        &mut self,
        input: RheiBuffer<Record>,
        _: &mut OperatorContext,
    ) -> anyhow::Result<BufferOutput<Record>> {
        Ok(BufferOutput::Single(input))
    }
    async fn snapshot(&mut self, ctx: &mut OperatorContext) -> anyhow::Result<()> {
        ctx.state.put(b"uncommitted", &1u64)?;
        anyhow::bail!("injected operator snapshot failure")
    }
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn unsupported_sources_and_failed_snapshots_never_publish() {
    for unsupported in [true, false] {
        let dir = tempfile::tempdir().unwrap();
        let graph = DataflowGraph::new();
        if unsupported {
            graph
                .source(UnsupportedSource)
                .sink(output(Arc::default(), false));
        } else {
            graph
                .source(WindowSource {
                    cursor: 0,
                    pause: true,
                })
                .operator("broken", BrokenSnapshot)
                .sink(output(Arc::default(), false));
        }
        let controller = PipelineController::builder()
            .checkpoint_dir(dir.path())
            .workers(3)
            .checkpoint_interval(1)
            .coordinated_recovery(true)
            .build()
            .unwrap();
        let error = tokio::time::timeout(Duration::from_secs(10), controller.run(graph))
            .await
            .unwrap()
            .unwrap_err();
        assert!(
            error.to_string().contains(if unsupported {
                "does not support"
            } else {
                "snapshot failure"
            }),
            "{error}"
        );
        assert!(!dir.path().join("recovery.json").exists());
        // Aborted pending source futures must release their attempt locks before
        // run returns, so an immediate restart is possible.
        tokio::time::timeout(
            Duration::from_secs(10),
            controller.run(self::graph(output(Arc::default(), false))),
        )
        .await
        .unwrap()
        .unwrap();
    }
}

#[cfg(feature = "remote-state")]
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn slate_pipeline_restarts_from_committed_clone_after_sink_failure() {
    use rhei_core::state::slatedb_backend::SlateDbBackend;
    use rhei_core::state::tiered_backend::TieredBackendConfig;
    let dir = tempfile::tempdir().unwrap();
    let store = Arc::new(object_store::memory::InMemory::new());
    let backend = Arc::new(SlateDbBackend::open("recovery-test", store).await.unwrap());
    let durable: Arc<Mutex<Vec<(String, u64)>>> = Arc::default();
    for (attempt, fail) in [true, false].into_iter().enumerate() {
        let controller = PipelineController::builder()
            .checkpoint_dir(dir.path())
            .workers(3)
            .checkpoint_interval(3)
            .coordinated_recovery(true)
            .build()
            .unwrap()
            .with_tiered_storage(
                dir.path().to_owned(),
                backend.clone(),
                TieredBackendConfig {
                    foyer_dir: dir.path().join(format!("cache-{attempt}")),
                    foyer_memory_capacity: 1024 * 1024,
                    foyer_disk_capacity: 16 * 1024 * 1024,
                    ..TieredBackendConfig::default()
                },
            )
            .await
            .unwrap();
        let result = tokio::time::timeout(
            Duration::from_secs(30),
            controller.run(graph(output(durable.clone(), fail))),
        )
        .await
        .unwrap();
        assert_eq!(result.is_err(), fail, "{result:?}");
    }
    backend.close().await.unwrap();
    let mut actual = durable.lock().unwrap().clone();
    actual.sort();
    let expected: Vec<_> = (0..3)
        .flat_map(|key| (1..=8).map(move |count| (format!("key-{key}"), count)))
        .collect();
    assert_eq!(actual, expected);
}
