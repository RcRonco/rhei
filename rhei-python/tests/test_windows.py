"""L3: tumbling-window aggregation with declarative agg specs."""

import pyarrow as pa
import pytest

import rhei

# Input rows: (user, ts_millis, amount)
SCHEMA = pa.schema(
    [("user", pa.string()), ("ts", pa.int64()), ("amount", pa.float64())]
)


def _batch(rows):
    users = pa.array([r[0] for r in rows], type=pa.string())
    ts = pa.array([r[1] for r in rows], type=pa.int64())
    amt = pa.array([r[2] for r in rows], type=pa.float64())
    return pa.record_batch([users, ts, amt], schema=SCHEMA)


def _run_window(batches, aggs, window_size_ms=10):
    """Run a tumbling window and capture emitted window rows as dicts."""
    captured = []
    df = rhei.Dataflow()
    (
        df.source(batches)
        .key_by(lambda r: r["user"])
        .tumbling_window(
            window_size_ms=window_size_ms,
            key_fn=lambda r: r["user"],
            time_fn=lambda r: r["ts"],
            aggs=aggs,
        )
        .inspect(lambda r: captured.append(dict(r)))
        .sink_print()
    )
    df.run(workers=1)
    return captured


def test_tumbling_window_count_and_sum():
    # window size 10ms. user a: ts 1,2,3 (window 0) amounts 10,20,30.
    #                   user b: ts 4 (window 0) amount 5; ts 12 (window 10) amount 7.
    batches = [
        _batch([("a", 1, 10.0), ("a", 2, 20.0), ("b", 4, 5.0)]),
        _batch([("a", 3, 30.0), ("b", 12, 7.0)]),
    ]
    rows = _run_window(batches, [rhei.Agg.count(), rhei.Agg.sum("amount")])

    # Index by (key, window_start).
    by_key = {(r["key"], r["window_start"]): r for r in rows}

    assert by_key[("a", 0)]["count"] == 3
    assert by_key[("a", 0)]["sum_amount"] == 60.0
    assert by_key[("b", 0)]["count"] == 1
    assert by_key[("b", 0)]["sum_amount"] == 5.0
    assert by_key[("b", 10)]["count"] == 1
    assert by_key[("b", 10)]["sum_amount"] == 7.0
    # window_end is start + size
    assert by_key[("a", 0)]["window_end"] == 10


def test_tumbling_window_min_max_mean():
    batches = [_batch([("a", 1, 10.0), ("a", 2, 30.0), ("a", 5, 20.0)])]
    rows = _run_window(
        batches,
        [rhei.Agg.min("amount"), rhei.Agg.max("amount"), rhei.Agg.mean("amount")],
    )
    assert len(rows) == 1
    r = rows[0]
    assert r["key"] == "a"
    assert r["min_amount"] == 10.0
    assert r["max_amount"] == 30.0
    assert r["mean_amount"] == 20.0  # (10+30+20)/3


def test_tumbling_window_custom_output_names():
    batches = [_batch([("a", 1, 10.0), ("a", 2, 20.0)])]
    rows = _run_window(
        batches,
        [rhei.Agg.count(name="n"), rhei.Agg.sum("amount", name="total")],
    )
    assert len(rows) == 1
    assert rows[0]["n"] == 2
    assert rows[0]["total"] == 30.0


def test_tumbling_window_separates_windows_by_time():
    # Same user, three different windows (size 10): ts 1, 15, 25.
    batches = [_batch([("a", 1, 1.0), ("a", 15, 2.0), ("a", 25, 3.0)])]
    rows = _run_window(batches, [rhei.Agg.sum("amount")])
    by_start = {r["window_start"]: r["sum_amount"] for r in rows}
    assert by_start == {0: 1.0, 10: 2.0, 20: 3.0}


def test_tumbling_window_rejects_nonpositive_size():
    df = rhei.Dataflow()
    s = df.source([_batch([("a", 1, 1.0)])])
    with pytest.raises(RuntimeError, match="window_size_ms must be positive"):
        s.tumbling_window(
            window_size_ms=0,
            key_fn=lambda r: r["user"],
            time_fn=lambda r: r["ts"],
            aggs=[rhei.Agg.count()],
        )


def test_tumbling_window_time_fn_error_aborts_run():
    batches = [_batch([("a", 1, 1.0)])]

    def bad_time(row):
        raise ValueError("time_fn boom")

    df = rhei.Dataflow()
    (
        df.source(batches)
        .tumbling_window(
            window_size_ms=10,
            key_fn=lambda r: r["user"],
            time_fn=bad_time,
            aggs=[rhei.Agg.count()],
        )
        .sink_print()
    )
    with pytest.raises(RuntimeError, match="time_fn boom"):
        df.run(workers=1)
