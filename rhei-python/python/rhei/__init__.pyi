"""Type stubs for the ``rhei`` package.

Re-exports the compiled ``_rhei`` symbols and declares the pure-Python base
classes (``Operator``, ``Source``, ``Sink``) defined in ``__init__.py``.
"""

from typing import Any

from ._rhei import (
    Agg as Agg,
    Buffer as Buffer,
    CollectHandle as CollectHandle,
    Dataflow as Dataflow,
    State as State,
    Stream as Stream,
    __version__ as __version__,
)

__all__ = [
    "Agg",
    "Buffer",
    "CollectHandle",
    "Dataflow",
    "Operator",
    "Sink",
    "Source",
    "State",
    "Stream",
    "__version__",
]

class Operator:
    """Base class for custom stateful operators.

    Subclass and implement ``process``; optionally override ``on_watermark``,
    ``on_timer``, ``open``, and ``close``. Attach with
    ``stream.operator("name", MyOp())``.
    """

    def process(self, buffer: Buffer, state: State) -> list[Buffer] | Buffer | None: ...
    # Optional hooks (define on a subclass to have them dispatched):
    #   def on_watermark(self, watermark: int, state: State) -> list[Buffer] | Buffer | None
    #   def on_timer(self, timestamp: int, key: str, state: State) -> list[Buffer] | Buffer | None
    #   def open(self, state: State) -> None
    #   def close(self) -> None

class Source:
    """Base class for custom Python sources.

    Subclass and implement ``next_batch`` returning the next batch or ``None``
    at end of stream. Attach with ``df.from_source(MySource())``.
    """

    def next_batch(self) -> Buffer | None: ...

class Sink:
    """Base class for custom Python sinks.

    Subclass and implement ``write_batch``; optionally override ``flush``.
    Attach with ``stream.sink(MySink())``.
    """

    def write_batch(self, buffer: Buffer) -> None: ...
    def flush(self) -> None: ...
