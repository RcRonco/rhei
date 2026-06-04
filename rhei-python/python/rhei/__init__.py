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

from ._rhei import Buffer, CollectHandle, Dataflow, Stream, __version__

__all__ = ["Buffer", "CollectHandle", "Dataflow", "Stream", "__version__"]
