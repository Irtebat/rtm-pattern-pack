"""Reference implementations used only by the tests."""

from __future__ import annotations

from typing import Iterable, Optional

from rtmpack_harness.stats import OutputAccumulator, rank_for


def nearest_rank_percentile(sorted_values: list[int], p: float) -> Optional[int]:
    """Nearest-rank percentile on a sorted list, to check the histogram against."""
    if not sorted_values:
        return None
    return int(sorted_values[rank_for(len(sorted_values), p) - 1])


def summarize_output(
    records: Iterable[tuple[bytes, int, int]],
    cutoff_ms: int,
    end_ms: int,
    expected_distinct: int,
    measured_s: float,
) -> dict:
    """Run OutputAccumulator over (key, in_ts, out_ts) tuples."""
    acc = OutputAccumulator(cutoff_ms, end_ms)
    for key, in_ts, out_ts in records:
        acc.offer(key, in_ts, out_ts)
    return acc.finish(expected_distinct, measured_s)
