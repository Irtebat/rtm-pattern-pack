"""06-tumbling: running count and sum per key in 60 s tumbling windows."""

from __future__ import annotations

from rtmpack_harness.patterns.windows import AmountGenerator, window_pattern

TUMBLE_MS = 60_000


def tumbling_windows(event_ts: int) -> tuple[int, ...]:
    """06-tumbling rule (patterns/06-tumbling/README.md, Benchmark setup): the 60 s tumbling window start, aligned to the epoch."""
    return (event_ts // TUMBLE_MS * TUMBLE_MS,)


class TumblingGenerator(AmountGenerator):
    pattern_id = "06-tumbling"


PATTERN = window_pattern("06-tumbling", "tumbling window count and sum per key", TumblingGenerator, tumbling_windows)
