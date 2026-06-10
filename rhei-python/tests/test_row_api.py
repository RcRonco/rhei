"""L2: stateless row API (map/filter/flat_map/inspect), key_by, merge."""

import pyarrow as pa
import pytest

import rhei

INT = pa.schema([("n", pa.int64())])


def _ints(values):
    return [pa.record_batch([pa.array(values, type=pa.int64())], schema=INT)]


def test_row_map():
    def double(row):
        return {"n": row["n"] * 2}

    df = rhei.Dataflow()
    c = df.source(_ints([1, 2, 3])).map(double, schema=INT).sink_collect()
    df.run(workers=1)
    assert sorted(c.values()) == [2, 4, 6]


def test_row_filter():
    df = rhei.Dataflow()
    c = df.source(_ints([1, 2, 3, 4, 5])).filter(lambda r: r["n"] % 2 == 0).sink_collect()
    df.run(workers=1)
    assert sorted(c.values()) == [2, 4]


def test_row_filter_then_map_sees_only_kept_rows():
    seen = []

    df = rhei.Dataflow()
    c = (
        df.source(_ints([1, 2, 3, 4]))
        .filter(lambda r: r["n"] > 2)
        .inspect(lambda r: seen.append(r["n"]))
        .map(lambda r: {"n": r["n"] * 10}, schema=INT)
        .sink_collect()
    )
    df.run(workers=1)
    assert sorted(c.values()) == [30, 40]
    # inspect downstream of filter must never see the filtered-out rows.
    assert sorted(seen) == [3, 4]


def test_row_flat_map():
    def explode(row):
        return [{"n": row["n"]}, {"n": row["n"]}]

    df = rhei.Dataflow()
    c = df.source(_ints([1, 2])).flat_map(explode, schema=INT).sink_collect()
    df.run(workers=1)
    assert sorted(c.values()) == [1, 1, 2, 2]


def test_row_flat_map_empty_drops_rows():
    df = rhei.Dataflow()
    c = df.source(_ints([1, 2, 3])).flat_map(lambda r: [], schema=INT).sink_collect()
    df.run(workers=1)
    assert c.values() == []


def test_row_map_schema_change_and_types():
    out = pa.schema([("label", pa.string()), ("doubled", pa.float64()), ("ok", pa.bool_())])

    def transform(row):
        return {"label": f"row-{row['n']}", "doubled": row["n"] * 2.0, "ok": row["n"] > 1}

    captured = []
    df = rhei.Dataflow()
    df.source(_ints([1, 2])).map(transform, schema=out).inspect(
        lambda r: captured.append((r["label"], r["doubled"], r["ok"]))
    ).sink_print()
    df.run(workers=1)
    assert sorted(captured) == [("row-1", 2.0, False), ("row-2", 4.0, True)]


def test_row_null_handling():
    schema = pa.schema([("n", pa.int64())])
    batches = [pa.record_batch([pa.array([1, None, 3], type=pa.int64())], schema=schema)]
    captured = []

    df = rhei.Dataflow()
    df.source(batches).inspect(lambda r: captured.append(r["n"])).sink_print()
    df.run(workers=1)
    assert captured == [1, None, 3]


def test_key_by_then_map_multi_worker():
    # key_by repartitions across workers; a downstream stateless map must still
    # see (and double) every row exactly once.
    df = rhei.Dataflow()
    c = (
        df.source(_ints(list(range(20))))
        .key_by(lambda r: str(r["n"] % 4))
        .map(lambda r: {"n": r["n"] * 2}, schema=INT)
        .sink_collect()
    )
    df.run(workers=3)
    assert sorted(c.values()) == [v * 2 for v in range(20)]


def test_merge_two_sources():
    df = rhei.Dataflow()
    a = df.source(_ints([1, 2, 3]))
    b = df.source(_ints([4, 5, 6]))
    c = a.merge(b).sink_collect()
    df.run(workers=1)
    assert sorted(c.values()) == [1, 2, 3, 4, 5, 6]


def test_row_map_exception_aborts_run():
    def boom(row):
        raise ValueError("row blew up")

    df = rhei.Dataflow()
    df.source(_ints([1])).map(boom, schema=INT).sink_collect()
    with pytest.raises(RuntimeError, match="row blew up"):
        df.run(workers=1)


def test_filter_predicate_must_return_bool():
    df = rhei.Dataflow()
    df.source(_ints([1, 2])).filter(lambda r: "not a bool").sink_collect()
    with pytest.raises(RuntimeError):
        df.run(workers=1)
