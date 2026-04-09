"""Rhei -- Python bindings for the Rhei stream processing engine.

Usage::

    import rhei

    pipeline = rhei.Pipeline()
    source = pipeline.source("input", rhei.VecSource([1, 2, 3, 4, 5]))
    mapped = source.map(lambda x: x * 2)
    filtered = mapped.filter(lambda x: x > 4)
    filtered.sink("output", rhei.PrintSink())
    pipeline.run(workers=1)
"""

from rhei._native import Pipeline, PrintSink, Stream, VecSource

__all__ = [
    "Pipeline",
    "Stream",
    "VecSource",
    "PrintSink",
]
