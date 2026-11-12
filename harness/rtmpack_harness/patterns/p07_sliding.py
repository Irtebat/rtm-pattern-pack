"""07-sliding: running count and sum per key in 60 s windows that slide every 20 s."""

from __future__ import annotations

from rtmpack_harness.patterns.windows import AmountGenerator, window_pattern

SLIDE_MS = 20_000


def sliding_windows(event_ts: int) -> tuple[int, ...]:
    """07-sliding rule (patterns/07-sliding/README.md, Benchmark setup): starts of the three 60 s windows (20 s slide) holding event_ts."""
    last = event_ts // SLIDE_MS * SLIDE_MS
    return (last - 2 * SLIDE_MS, last - SLIDE_MS, last)


class SlidingGenerator(AmountGenerator):
    pattern_id = "07-sliding"


PATTERN = window_pattern("07-sliding", "sliding window count and sum per key", SlidingGenerator, sliding_windows)
