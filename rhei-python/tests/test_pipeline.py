import pyarrow as pa
import rhei


def test_buffer_roundtrips_arrow_zero_copy():
    schema = pa.schema([("n", pa.int64())])
    rb = pa.record_batch([pa.array([1, 2, 3], type=pa.int64())], schema=schema)

    buf = rhei.Buffer.from_arrow(rb)
    assert len(buf) == 3

    out = buf.to_arrow()
    assert out.num_rows == 3
    assert out.column(0).to_pylist() == [1, 2, 3]
    assert out.schema == schema


def test_map_batches_pipeline_runs_end_to_end():
    schema = pa.schema([("n", pa.int64())])
    batches = [
        pa.record_batch([pa.array([1, 2, 3], type=pa.int64())], schema=schema),
        pa.record_batch([pa.array([4, 5], type=pa.int64())], schema=schema),
    ]

    def double(buf):
        rb = buf.to_arrow()
        doubled = pa.array([v * 2 for v in rb.column(0).to_pylist()], type=pa.int64())
        return rhei.Buffer.from_arrow(pa.record_batch([doubled], schema=schema))

    df = rhei.Dataflow()
    collected = df.source(batches).map_batches(double, schema=schema).sink_collect()
    df.run(workers=1)

    assert sorted(collected.values()) == [2, 4, 6, 8, 10]


def test_print_sink_runs(capfd):
    schema = pa.schema([("n", pa.int64())])
    batches = [pa.record_batch([pa.array([7, 8], type=pa.int64())], schema=schema)]

    df = rhei.Dataflow()
    df.source(batches).sink_print()
    df.run(workers=1)

    out, _ = capfd.readouterr()
    assert "7" in out and "8" in out


def test_multiple_map_batches_chain():
    schema = pa.schema([("n", pa.int64())])
    batches = [pa.record_batch([pa.array([1, 2, 3], type=pa.int64())], schema=schema)]

    def add_one(buf):
        rb = buf.to_arrow()
        col = pa.array([v + 1 for v in rb.column(0).to_pylist()], type=pa.int64())
        return rhei.Buffer.from_arrow(pa.record_batch([col], schema=schema))

    def times_ten(buf):
        rb = buf.to_arrow()
        col = pa.array([v * 10 for v in rb.column(0).to_pylist()], type=pa.int64())
        return rhei.Buffer.from_arrow(pa.record_batch([col], schema=schema))

    df = rhei.Dataflow()
    collected = (
        df.source(batches)
        .map_batches(add_one, schema=schema)
        .map_batches(times_ten, schema=schema)
        .sink_collect()
    )
    df.run(workers=1)

    # (1,2,3) -> +1 -> (2,3,4) -> *10 -> (20,30,40)
    assert sorted(collected.values()) == [20, 30, 40]
