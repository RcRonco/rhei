"""L5: Python sources/sinks + hardening (error policy, DLQ, metrics, run opts)."""

import json

import pyarrow as pa
import pytest

import rhei

INT = pa.schema([("n", pa.int64())])


def _batch(values):
    return pa.record_batch([pa.array(values, type=pa.int64())], schema=INT)


def test_python_source_and_sink():
    class CountingSource(rhei.Source):
        def __init__(self, batches):
            self._batches = list(batches)
            self._i = 0

        def next_batch(self):
            if self._i >= len(self._batches):
                return None
            b = self._batches[self._i]
            self._i += 1
            return rhei.Buffer.from_arrow(b)

    class CollectingSink(rhei.Sink):
        def __init__(self):
            self.rows = []
            self.flushed = False

        def write_batch(self, buffer):
            self.rows.extend(buffer.to_arrow().column(0).to_pylist())

        def flush(self):
            self.flushed = True

    sink = CollectingSink()
    df = rhei.Dataflow()
    df.from_source(CountingSource([_batch([1, 2, 3]), _batch([4, 5])])).sink(sink)
    df.run(workers=1)

    assert sorted(sink.rows) == [1, 2, 3, 4, 5]
    assert sink.flushed


def test_python_source_into_row_api():
    class OneShot(rhei.Source):
        def __init__(self):
            self._done = False

        def next_batch(self):
            if self._done:
                return None
            self._done = True
            return rhei.Buffer.from_arrow(_batch([1, 2, 3, 4]))

    df = rhei.Dataflow()
    c = (
        df.from_source(OneShot())
        .filter(lambda r: r["n"] % 2 == 0)
        .map(lambda r: {"n": r["n"] * 100}, schema=INT)
        .sink_collect()
    )
    df.run(workers=1)
    assert sorted(c.values()) == [200, 400]


def test_python_source_exception_aborts_run():
    class BadSource(rhei.Source):
        def next_batch(self):
            raise ValueError("source boom")

    df = rhei.Dataflow()
    df.from_source(BadSource()).sink_print()
    with pytest.raises(RuntimeError, match="source boom"):
        df.run(workers=1)


def test_python_sink_exception_aborts_run():
    class BadSink(rhei.Sink):
        def write_batch(self, buffer):
            raise ValueError("sink boom")

    df = rhei.Dataflow()
    df.source([_batch([1])]).sink(BadSink())
    with pytest.raises(RuntimeError, match="sink boom"):
        df.run(workers=1)


def test_run_rejects_bad_on_error():
    df = rhei.Dataflow()
    df.source([_batch([1])]).sink_print()
    with pytest.raises(RuntimeError, match='on_error must be'):
        df.run(workers=1, on_error="explode")


def test_run_dlq_requires_path():
    df = rhei.Dataflow()
    df.source([_batch([1])]).sink_print()
    with pytest.raises(RuntimeError, match="requires dlq_path"):
        df.run(workers=1, on_error="dlq")


def test_run_rejects_bad_metrics_addr():
    df = rhei.Dataflow()
    df.source([_batch([1])]).sink_print()
    with pytest.raises(RuntimeError, match="invalid metrics_addr"):
        df.run(workers=1, metrics_addr="not-an-address")


def test_run_with_explicit_skip_policy_succeeds():
    sink_rows = []

    class Collect(rhei.Sink):
        def write_batch(self, buffer):
            sink_rows.extend(buffer.to_arrow().column(0).to_pylist())

    df = rhei.Dataflow()
    df.source([_batch([1, 2, 3])]).sink(Collect())
    df.run(workers=1, on_error="skip")
    assert sorted(sink_rows) == [1, 2, 3]


def test_run_dlq_path_accepted(tmp_path):
    # A clean run with a DLQ configured (no errors) should succeed and create
    # the DLQ file.
    dlq = tmp_path / "dlq.jsonl"
    df = rhei.Dataflow()
    df.source([_batch([1, 2, 3])]).sink_print()
    df.run(workers=1, on_error="dlq", dlq_path=str(dlq))
    assert dlq.exists()
    # No errors, so the DLQ is empty.
    contents = dlq.read_text().strip()
    assert contents == "" or all(json.loads(line) for line in contents.splitlines())
