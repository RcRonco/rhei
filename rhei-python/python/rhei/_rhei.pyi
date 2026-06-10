"""Type stubs for the compiled ``rhei._rhei`` extension module.

These mirror the PyO3 ``#[pymethods]`` in ``rhei-python/src``. Keep them in sync
with the Rust definitions; ``tests/test_stubs.py`` checks they don't drift.
"""

from collections.abc import Callable, Iterable
from typing import Any

import pyarrow as pa

__version__: str

class Buffer:
    """An Arrow batch flowing through a rhei pipeline.

    Construct from a pyarrow ``RecordBatch`` and read it back; both are
    zero-copy via the Arrow C Data Interface.
    """

    @staticmethod
    def from_arrow(batch: pa.RecordBatch) -> Buffer: ...
    def to_arrow(self) -> pa.RecordBatch: ...
    @property
    def num_rows(self) -> int: ...
    def __len__(self) -> int: ...
    def __repr__(self) -> str: ...

class CollectHandle:
    """Handle returned by ``Stream.sink_collect()``; exposes collected values
    after ``run()``. Reads the first ``Int64`` column of every batch."""

    def values(self) -> list[int]: ...
    def __repr__(self) -> str: ...

class State:
    """Short-lived keyed-state handle passed to operator callbacks.

    Valid only during the call it was passed to; using it afterward raises
    ``RuntimeError``. State is namespaced per operator and survives
    checkpoint/restart.
    """

    def get(self, key: bytes) -> bytes | None: ...
    def put(self, key: bytes, value: bytes) -> None: ...
    def delete(self, key: bytes) -> None: ...
    def register_timer(self, timestamp: int, key: str) -> None: ...

class Agg:
    """Declarative aggregation spec for ``Stream.tumbling_window``."""

    @staticmethod
    def count(name: str | None = ...) -> Agg: ...
    @staticmethod
    def sum(column: str, name: str | None = ...) -> Agg: ...
    @staticmethod
    def min(column: str, name: str | None = ...) -> Agg: ...
    @staticmethod
    def max(column: str, name: str | None = ...) -> Agg: ...
    @staticmethod
    def mean(column: str, name: str | None = ...) -> Agg: ...

class Stream:
    """A point in a rhei dataflow. Operations add nodes and return new handles."""

    # ── Batch transforms ──────────────────────────────────────────────
    def map_batches(
        self, func: Callable[[Buffer], Buffer], schema: pa.Schema
    ) -> Stream: ...

    # ── Row transforms (row is a dict keyed by field name) ────────────
    def map(
        self, func: Callable[[dict[str, Any]], dict[str, Any]], schema: pa.Schema
    ) -> Stream: ...
    def filter(self, func: Callable[[dict[str, Any]], bool]) -> Stream: ...
    def flat_map(
        self,
        func: Callable[[dict[str, Any]], Iterable[dict[str, Any]]],
        schema: pa.Schema,
    ) -> Stream: ...
    def inspect(self, func: Callable[[dict[str, Any]], None]) -> Stream: ...

    # ── Structure ─────────────────────────────────────────────────────
    def key_by(self, func: Callable[[dict[str, Any]], str]) -> Stream: ...
    def merge(self, other: Stream) -> Stream: ...

    # ── Stateful operators & windows ──────────────────────────────────
    def operator(self, name: str, op: Any) -> Stream: ...
    def tumbling_window(
        self,
        window_size_ms: int,
        key_fn: Callable[[dict[str, Any]], str],
        time_fn: Callable[[dict[str, Any]], int],
        aggs: list[Agg],
    ) -> Stream: ...

    # ── Sinks (terminal) ──────────────────────────────────────────────
    def sink_collect(self) -> CollectHandle: ...
    def sink_print(self) -> None: ...
    def sink(self, sink: Any) -> None: ...

class Dataflow:
    """A rhei dataflow graph builder."""

    def __init__(self) -> None: ...
    def source(self, batches: Iterable[pa.RecordBatch]) -> Stream: ...
    def from_source(self, source: Any) -> Stream: ...
    def run(
        self,
        workers: int = ...,
        checkpoint_dir: str | None = ...,
        metrics_addr: str | None = ...,
        on_error: str = ...,
        dlq_path: str | None = ...,
        handle_sigint: bool = ...,
    ) -> None: ...
