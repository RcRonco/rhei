"""Row API + tumbling-window aggregation + a custom stateful operator.

Run with:

    cd rhei-python
    uv run maturin develop
    uv run python examples/windowed_sales.py
"""

import pyarrow as pa

import rhei

# Sales events: (user, ts_millis, amount).
SCHEMA = pa.schema(
    [("user", pa.string()), ("ts", pa.int64()), ("amount", pa.float64())]
)


def _batch(rows):
    return pa.record_batch(
        [
            pa.array([r[0] for r in rows], type=pa.string()),
            pa.array([r[1] for r in rows], type=pa.int64()),
            pa.array([r[2] for r in rows], type=pa.float64()),
        ],
        schema=SCHEMA,
    )


batches = [
    _batch([("alice", 1, 10.0), ("bob", 2, 5.0), ("alice", 3, 20.0)]),
    _batch([("alice", 12, 7.0), ("bob", 14, 3.0)]),
]


def main() -> None:
    df = rhei.Dataflow()
    (
        df.source(batches)
        # Row API: drop tiny sales.
        .filter(lambda r: r["amount"] >= 5.0)
        # Repartition by user so each key aggregates on one worker.
        .key_by(lambda r: r["user"])
        # 10ms tumbling windows: per-user count + total + average sale.
        .tumbling_window(
            window_size_ms=10,
            key_fn=lambda r: r["user"],
            time_fn=lambda r: r["ts"],
            aggs=[
                rhei.Agg.count(name="num_sales"),
                rhei.Agg.sum("amount", name="total"),
                rhei.Agg.mean("amount", name="avg"),
            ],
        )
        .sink_print()
    )
    df.run(workers=2)


if __name__ == "__main__":
    main()
