"""Latency percentiles, distinct counting and correctness metrics."""

import numpy as np
import pytest

from rtmpack_harness.stats import (
    DistinctCounter,
    LatencyHistogram,
    latency_ms,
)

from helpers import nearest_rank_percentile, summarize_output


def test_latency_is_output_minus_input_append_time():
    assert latency_ms(out_ts_ms=1_700_000_000_250, in_ts_ms=1_700_000_000_000) == 250


def test_nearest_rank_reference_values():
    values = [10, 20, 30, 40, 50, 60, 70, 80, 90, 100]
    assert nearest_rank_percentile(values, 50) == 50
    assert nearest_rank_percentile(values, 95) == 100
    assert nearest_rank_percentile(values, 10) == 10
    assert nearest_rank_percentile(values, 0.0001) == 10
    assert nearest_rank_percentile([], 50) is None


def test_histogram_percentiles_match_nearest_rank_reference():
    rng = np.random.default_rng(0)
    values = rng.integers(0, 5000, size=20_000)
    hist = LatencyHistogram()
    for chunk in np.array_split(values, 7):
        hist.add(chunk)
    ordered = sorted(int(v) for v in values)
    for p in (50, 95, 99, 99.9):
        assert hist.percentile(p) == nearest_rank_percentile(ordered, p), p
    assert hist.max_ms == max(ordered)
    assert hist.total == len(values)


def test_histogram_grows_for_large_latencies():
    hist = LatencyHistogram()
    hist.add([1, 2, 3])
    hist.add([900_000])
    assert hist.percentile(100) == 900_000
    assert hist.max_ms == 900_000


def test_histogram_excludes_negative_latencies_and_counts_them():
    hist = LatencyHistogram()
    hist.add([-5, 10, 20])
    assert hist.negative == 1
    assert hist.total == 2
    assert hist.percentile(50) == 10


def test_empty_histogram_has_no_percentiles():
    hist = LatencyHistogram()
    assert hist.percentile(50) is None
    assert hist.max_ms is None


def test_distinct_counter_counts_exactly_and_is_repeatable():
    counter = DistinctCounter()
    counter.add([b"a", b"b", b"a"])
    counter.add([b"c", b"b"])
    assert counter.count() == 3
    assert counter.count() == 3
    counter.add([b"d"])
    assert counter.count() == 4


def test_summarize_output_correctness_math():
    cutoff, end = 1_000, 10_000
    expected = 4
    records = [
        (b"e1", 1_100, 1_150),   # in window, latency 50
        (b"e2", 1_200, 1_300),   # in window, latency 100
        (b"e2", 1_200, 1_400),   # sink duplicate of e2, latency 200
        (b"e3", 1_500, 1_520),   # in window, latency 20
        (b"e9", 500, 600),       # before warm-up cutoff: excluded
        (b"e8", 20_000, 20_010), # after run end: excluded
    ]
    out = summarize_output(records, cutoff, end, expected, measured_s=9.0)
    assert out["out_records"] == 4
    assert out["distinct_out_keys"] == 3
    assert out["expected_distinct"] == 4
    assert out["missing"] == 1
    assert out["sink_duplicates"] == 1
    assert out["excluded_records"] == 2
    assert out["max_ms"] == 200
    assert out["p50_ms"] == 50
    assert out["p99_ms"] == 200
    assert out["throughput_eps"] == round(4 / 9.0, 2)


def test_summarize_output_handles_no_records():
    out = summarize_output([], 0, 1_000, 0, measured_s=1.0)
    assert out["out_records"] == 0
    assert out["p50_ms"] is None
    assert out["p999_ms"] is None
    assert out["missing"] == 0
    assert out["sink_duplicates"] == 0


def test_summarize_output_zero_measured_time_gives_zero_throughput():
    out = summarize_output([(b"k", 5, 9)], 0, 10, 1, measured_s=0.0)
    assert out["throughput_eps"] == 0.0


@pytest.mark.parametrize("expected,distinct,missing", [(10, 10, 0), (10, 7, 3)])
def test_missing_is_expected_minus_distinct(expected, distinct, missing):
    records = [(str(i).encode(), 100, 110) for i in range(distinct)]
    out = summarize_output(records, 0, 1_000, expected, measured_s=1.0)
    assert out["missing"] == missing


def test_correctness_window_is_independent_of_latency_window():
    from rtmpack_harness.stats import OutputAccumulator

    acc = OutputAccumulator(cutoff_ms=1_000, end_ms=2_000, correct_from_ms=0, correct_until_ms=2_500)
    acc.offer(b"warmup-event", in_ts=500, out_ts=510)    # counted for correctness only
    acc.offer(b"measured-event", in_ts=1_500, out_ts=1_520)  # counted for both
    acc.offer(b"late-dup", in_ts=2_400, out_ts=2_405)    # correctness only (after end)
    acc.offer(b"probe", in_ts=-10, out_ts=0)             # outside both windows
    stats = acc.finish(expected_distinct=3, measured_s=1.0)
    assert stats["out_records"] == 3
    assert stats["distinct_out_keys"] == 3
    assert stats["missing"] == 0
    assert stats["max_ms"] == 20
    assert stats["throughput_eps"] == 1.0
    assert stats["excluded_records"] == 1


# ---- split-half stability (contract section 8)

from rtmpack_harness.stats import OutputAccumulator, relative_diff_ok, split_half_stable  # noqa: E402


def test_relative_diff_uses_the_larger_value_and_includes_the_boundary():
    assert relative_diff_ok(85, 100)       # exactly 15 percent of 100
    assert not relative_diff_ok(84, 100)
    assert relative_diff_ok(100, 85)
    assert not relative_diff_ok(100, 84)
    assert relative_diff_ok(0, 0)
    assert not relative_diff_ok(0, 1)
    assert not relative_diff_ok(None, 5)


def test_split_half_stable_needs_both_percentiles_within_tolerance():
    assert split_half_stable(100, 400, 90, 360)
    assert not split_half_stable(100, 400, 70, 400)   # p50 off
    assert not split_half_stable(100, 400, 100, 300)  # p99 off
    assert not split_half_stable(None, None, None, None)


def test_accumulator_splits_the_window_by_in_ts():
    acc = OutputAccumulator(0, 1_000)
    for i in range(500):               # in_ts 0..499 -> first half, latency 10
        acc.offer(b"a%d" % i, i, i + 10)
    for i in range(500, 1000):         # in_ts 500..999 -> second half, latency 40
        acc.offer(b"b%d" % i, i, i + 40)
    stats = acc.finish(1000, 1.0)
    assert (stats["p50_ms_h1"], stats["p99_ms_h1"]) == (10, 10)
    assert (stats["p50_ms_h2"], stats["p99_ms_h2"]) == (40, 40)
    assert stats["stable"] is False
    assert stats["p50_ms"] == 10 and stats["max_ms"] == 40  # overall histogram holds both halves


def test_accumulator_reports_stable_for_similar_halves_and_not_for_an_empty_half():
    acc = OutputAccumulator(0, 1_000)
    for i in range(1000):
        acc.offer(b"k%d" % i, i, i + (20 if i < 500 else 22))
    assert acc.finish(1000, 1.0)["stable"] is True
    acc = OutputAccumulator(0, 1_000)
    acc.offer(b"k", 10, 20)
    stats = acc.finish(1, 1.0)
    assert stats["p50_ms_h2"] is None and stats["stable"] is False


def test_halves_ignore_records_outside_the_measured_window():
    acc = OutputAccumulator(100, 200, correct_from_ms=0, correct_until_ms=300)
    acc.offer(b"early", 50, 5000)      # counts for correctness only
    acc.offer(b"mid", 150, 160)
    stats = acc.finish(2, 1.0)
    assert stats["out_records"] == 2
    assert stats["p50_ms_h1"] is None and stats["p50_ms_h2"] == 10


def test_distinct_counter_is_exact_across_buckets_and_chunks():
    counter = DistinctCounter()
    keys = [b"key-%d" % i for i in range(5000)]
    counter.add(keys)
    counter.add(keys[:1000])
    counter.add([])
    assert counter.count() == 5000
    assert counter.count() == 5000  # counting twice gives the same answer
    counter.add([b"new"])
    assert counter.count() == 5001
