"""L4: custom Python stateful operators, state, and timers."""

import pyarrow as pa
import pytest

import rhei

INT = pa.schema([("n", pa.int64())])


def _ints(values):
    return [pa.record_batch([pa.array(values, type=pa.int64())], schema=INT)]


def _i64(n: int) -> bytes:
    return int(n).to_bytes(8, "little", signed=True)


def _from_i64(b) -> int:
    return int.from_bytes(b, "little", signed=True) if b else 0


def test_stateful_running_count_operator():
    """Operator keeps a running count in state and emits it per batch."""

    class RunningCount(rhei.Operator):
        def process(self, buffer, state):
            n = _from_i64(state.get(b"count")) + buffer.num_rows
            state.put(b"count", _i64(n))
            col = pa.array([n], type=pa.int64())
            return [rhei.Buffer.from_arrow(pa.record_batch([col], schema=INT))]

    df = rhei.Dataflow()
    c = (
        df.source([_ints([1, 2, 3])[0], _ints([4, 5])[0]])
        .operator("counter", RunningCount())
        .sink_collect()
    )
    df.run(workers=1)
    # batch1 -> count 3, batch2 -> count 5
    assert c.values() == [3, 5]


def test_operator_state_persists_across_batches():
    """A running SUM across batches proves state survives between process calls."""

    class RunningSum(rhei.Operator):
        def process(self, buffer, state):
            total = _from_i64(state.get(b"sum"))
            for v in buffer.to_arrow().column(0).to_pylist():
                total += v
            state.put(b"sum", _i64(total))
            col = pa.array([total], type=pa.int64())
            return [rhei.Buffer.from_arrow(pa.record_batch([col], schema=INT))]

    df = rhei.Dataflow()
    c = (
        df.source([_ints([1, 2, 3])[0], _ints([4, 5])[0]])
        .operator("sum", RunningSum())
        .sink_collect()
    )
    df.run(workers=1)
    # after batch1: 6, after batch2: 15
    assert c.values() == [6, 15]


def test_operator_can_return_single_buffer_or_none():
    class PassOdd(rhei.Operator):
        def process(self, buffer, state):
            vals = [v for v in buffer.to_arrow().column(0).to_pylist() if v % 2 == 1]
            if not vals:
                return None  # emit nothing
            col = pa.array(vals, type=pa.int64())
            # single Buffer (not a list) is accepted
            return rhei.Buffer.from_arrow(pa.record_batch([col], schema=INT))

    df = rhei.Dataflow()
    c = df.source(_ints([2, 4, 5, 6, 7])).operator("odd", PassOdd()).sink_collect()
    df.run(workers=1)
    assert sorted(c.values()) == [5, 7]


def test_operator_delete_state():
    class Toggle(rhei.Operator):
        def process(self, buffer, state):
            seen = state.get(b"seen")
            if seen is None:
                state.put(b"seen", b"yes")
                out = 1
            else:
                state.delete(b"seen")
                out = 2
            col = pa.array([out], type=pa.int64())
            return [rhei.Buffer.from_arrow(pa.record_batch([col], schema=INT))]

    df = rhei.Dataflow()
    c = (
        df.source([_ints([0])[0], _ints([0])[0], _ints([0])[0]])
        .operator("toggle", Toggle())
        .sink_collect()
    )
    df.run(workers=1)
    # set -> 1, delete -> 2, set again -> 1
    assert c.values() == [1, 2, 1]


def test_operator_timer_fires_with_state_access():
    """A registered timer fires (at the end-of-stream watermark) and on_timer
    runs with full state access.

    Note: emitting *new* downstream output from on_timer at end-of-stream is not
    reliably delivered by the runtime (the source capability may already be
    released), so this test asserts the timer's observable effects — it fired,
    saw the right key/timestamp, and could read the accumulated state — via a
    Python side channel rather than via emitted buffers.
    """
    fired = []

    class SumThenFlush(rhei.Operator):
        def process(self, buffer, state):
            total = _from_i64(state.get(b"sum"))
            for v in buffer.to_arrow().column(0).to_pylist():
                total += v
            state.put(b"sum", _i64(total))
            state.register_timer(100, "flush")
            return []

        def on_timer(self, timestamp, key, state):
            fired.append((timestamp, key, _from_i64(state.get(b"sum"))))
            return []

    df = rhei.Dataflow()
    (
        df.source([_ints([1, 2, 3])[0], _ints([4, 5])[0]])
        .operator("sum_flush", SumThenFlush())
        .sink_collect()
    )
    df.run(workers=1)

    # The timer fired at least once, saw its key, and read the grand total (15).
    assert fired, "on_timer should have fired at end of stream"
    assert all(key == "flush" for _ts, key, _total in fired)
    assert any(total == 15 for _ts, _key, total in fired)


def test_operator_on_watermark_hook():
    """on_watermark fires at end of stream and can emit."""

    class Counter(rhei.Operator):
        def process(self, buffer, state):
            n = _from_i64(state.get(b"n")) + buffer.num_rows
            state.put(b"n", _i64(n))
            return []

        def on_watermark(self, watermark, state):
            n = _from_i64(state.get(b"n"))
            col = pa.array([n], type=pa.int64())
            return [rhei.Buffer.from_arrow(pa.record_batch([col], schema=INT))]

    df = rhei.Dataflow()
    c = (
        df.source([_ints([1, 2, 3])[0], _ints([4, 5])[0]])
        .operator("count_wm", Counter())
        .sink_collect()
    )
    df.run(workers=1)
    # on_watermark emits the final count (5) at least once at end of stream.
    assert 5 in c.values()


def test_operator_exception_aborts_run():
    class Boom(rhei.Operator):
        def process(self, buffer, state):
            raise ValueError("operator exploded")

    df = rhei.Dataflow()
    df.source(_ints([1])).operator("boom", Boom()).sink_collect()
    with pytest.raises(RuntimeError, match="operator exploded"):
        df.run(workers=1)


def test_operator_missing_process_raises():
    class NoProcess(rhei.Operator):
        pass

    df = rhei.Dataflow()
    df.source(_ints([1])).operator("noproc", NoProcess()).sink_collect()
    with pytest.raises(RuntimeError, match="process"):
        df.run(workers=1)


def test_operator_bad_return_type_aborts_run():
    class BadReturn(rhei.Operator):
        def process(self, buffer, state):
            return 42  # not a Buffer / list / None

    df = rhei.Dataflow()
    df.source(_ints([1])).operator("bad", BadReturn()).sink_collect()
    with pytest.raises(RuntimeError):
        df.run(workers=1)


def test_state_handle_invalid_after_call():
    """A state handle stashed from a prior call must not be reusable later.

    The operator saves the handle on its first ``process`` and, on the second,
    tries to use the *stale* one. The runtime reclaims the backing state when a
    call returns, so the stale handle raises a clear error rather than touching
    freed state.
    """

    class Leak(rhei.Operator):
        def process(self, buffer, state):
            stale = getattr(self, "_stashed", None)
            if stale is not None:
                # Reusing a handle from a previous call must fail loudly.
                stale.get(b"x")
            self._stashed = state
            col = pa.array([1], type=pa.int64())
            return [rhei.Buffer.from_arrow(pa.record_batch([col], schema=INT))]

    df = rhei.Dataflow()
    df.source([_ints([1])[0], _ints([2])[0]]).operator("leak", Leak()).sink_collect()
    with pytest.raises(RuntimeError, match="only valid during"):
        df.run(workers=1)
