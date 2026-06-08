"""rhei — Python bindings for the rhei stream-processing framework.

Build a pipeline over Arrow batches and run it on the rhei runtime:

    import pyarrow as pa
    import rhei

    schema = pa.schema([("n", pa.int64())])
    batches = [pa.record_batch([pa.array([1, 2, 3], type=pa.int64())], schema=schema)]

    def double(buf):
        rb = buf.to_arrow()
        col = pa.array([v * 2 for v in rb.column(0).to_pylist()], type=pa.int64())
        return rhei.Buffer.from_arrow(pa.record_batch([col], schema=schema))

    df = rhei.Dataflow()
    df.source(batches).map_batches(double, schema=schema).sink_print()
    df.run(workers=1)
"""

from ._rhei import Agg, Buffer, CollectHandle, Dataflow, State, Stream, __version__


class Operator:
    """Base class for custom stateful operators.

    Subclass and implement ``process``; optionally override ``on_watermark``,
    ``on_timer``, ``open``, and ``close``. Attach an instance with
    ``stream.operator("name", MyOp())``.

    Each method (except ``close``) receives a ``state`` handle exposing
    ``get(key) -> bytes | None``, ``put(key, value)``, ``delete(key)``, and
    ``register_timer(timestamp, key)``. The handle is valid only for the
    duration of that call; do not store it. State is namespaced per operator
    and survives checkpoint/restart.

    Methods that emit downstream return a ``list`` of ``rhei.Buffer`` (or a
    single ``Buffer``, or ``None`` for nothing).

    Only the methods you define are called — the base class intentionally does
    not provide no-op defaults for the optional hooks, so the runtime can detect
    which you implemented.
    """

    def process(self, buffer, state):  # noqa: D401
        """Handle an input ``Buffer``. Override this."""
        raise NotImplementedError(
            f"{type(self).__name__}.process(self, buffer, state) is not implemented"
        )


__all__ = [
    "Agg",
    "Buffer",
    "CollectHandle",
    "Dataflow",
    "Operator",
    "State",
    "Stream",
    "__version__",
]
