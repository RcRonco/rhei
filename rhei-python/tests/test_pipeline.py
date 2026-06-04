import pyarrow as pa
import pytest

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


def test_multi_worker_stateless_map():
    schema = pa.schema([("n", pa.int64())])
    batches = [
        pa.record_batch([pa.array(list(range(1, 11)), type=pa.int64())], schema=schema)
    ]

    def double(buf):
        rb = buf.to_arrow()
        col = pa.array([v * 2 for v in rb.column(0).to_pylist()], type=pa.int64())
        return rhei.Buffer.from_arrow(pa.record_batch([col], schema=schema))

    df = rhei.Dataflow()
    collected = df.source(batches).map_batches(double, schema=schema).sink_collect()
    df.run(workers=2)

    assert sorted(collected.values()) == [2, 4, 6, 8, 10, 12, 14, 16, 18, 20]


def test_udf_exception_aborts_run():
    schema = pa.schema([("n", pa.int64())])
    batches = [pa.record_batch([pa.array([1, 2, 3], type=pa.int64())], schema=schema)]

    def boom(buf):
        raise ValueError("intentional UDF failure")

    df = rhei.Dataflow()
    df.source(batches).map_batches(boom, schema=schema).sink_collect()
    with pytest.raises(RuntimeError, match="intentional UDF failure"):
        df.run(workers=1)


def test_map_batches_schema_mismatch_aborts_run():
    in_schema = pa.schema([("n", pa.int64())])
    wrong_schema = pa.schema([("s", pa.string())])
    batches = [pa.record_batch([pa.array([1, 2, 3], type=pa.int64())], schema=in_schema)]

    def to_string(buf):
        # Returns a Utf8 column, but we declare an Int64 output schema below.
        col = pa.array(["a", "b", "c"], type=pa.string())
        return rhei.Buffer.from_arrow(pa.record_batch([col], schema=wrong_schema))

    df = rhei.Dataflow()
    # Declared output schema (int64 "n") does NOT match what the UDF produces.
    df.source(batches).map_batches(to_string, schema=in_schema).sink_collect()
    with pytest.raises(RuntimeError, match="does not match the declared"):
        df.run(workers=1)


def test_map_batches_wrong_return_type_aborts_run():
    schema = pa.schema([("n", pa.int64())])
    batches = [pa.record_batch([pa.array([1, 2, 3], type=pa.int64())], schema=schema)]

    def not_a_buffer(buf):
        return 42

    df = rhei.Dataflow()
    df.source(batches).map_batches(not_a_buffer, schema=schema).sink_collect()
    with pytest.raises(RuntimeError, match="must return a rhei.Buffer"):
        df.run(workers=1)


def test_sink_collect_non_int64_first_column_errors():
    schema = pa.schema([("s", pa.string())])
    batches = [pa.record_batch([pa.array(["x", "y"], type=pa.string())], schema=schema)]

    df = rhei.Dataflow()
    df.source(batches).sink_collect()
    with pytest.raises(RuntimeError, match="Int64 first column"):
        df.run(workers=1)


def test_run_twice_raises():
    schema = pa.schema([("n", pa.int64())])
    batches = [pa.record_batch([pa.array([1], type=pa.int64())], schema=schema)]

    df = rhei.Dataflow()
    df.source(batches).sink_collect()
    df.run(workers=1)
    with pytest.raises(RuntimeError, match="already been run"):
        df.run(workers=1)
