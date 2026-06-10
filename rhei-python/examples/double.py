"""Minimal rhei pipeline: double an Int64 column and print the result.

Run with:

    cd rhei-python
    uv run maturin develop
    uv run python examples/double.py
"""

import pyarrow as pa

import rhei

schema = pa.schema([("n", pa.int64())])
batches = [
    pa.record_batch([pa.array([1, 2, 3], type=pa.int64())], schema=schema),
    pa.record_batch([pa.array([4, 5], type=pa.int64())], schema=schema),
]


def double(buf: "rhei.Buffer") -> "rhei.Buffer":
    rb = buf.to_arrow()
    col = pa.array([v * 2 for v in rb.column(0).to_pylist()], type=pa.int64())
    return rhei.Buffer.from_arrow(pa.record_batch([col], schema=schema))


def main() -> None:
    df = rhei.Dataflow()
    df.source(batches).map_batches(double, schema=schema).sink_print()
    df.run(workers=1)


if __name__ == "__main__":
    main()
