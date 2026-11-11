"""Latency and correctness math for the collector.

Latency is kept in an integer-millisecond histogram, so percentiles are exact at
1 ms resolution and memory does not grow with the record count. Distinct keys are
counted exactly from 128-bit BLAKE2b digests of the Kafka record key.
"""

from __future__ import annotations

import hashlib
import math
from typing import Iterable, Optional

import numpy as np


def latency_ms(out_ts_ms: int, in_ts_ms: int) -> int:
    """Contract section 4: output append time minus input append time."""
    return int(out_ts_ms) - int(in_ts_ms)


def rank_for(total: int, p: float) -> int:
    """Nearest-rank index (1-based) for percentile p over `total` samples."""
    return max(1, math.ceil(round(total * p / 100.0, 9)))


def percentile_from_counts(counts: np.ndarray, total: int, p: float) -> Optional[int]:
    if total <= 0:
        return None
    cumulative = np.cumsum(counts)
    return int(np.searchsorted(cumulative, rank_for(total, p), side="left"))


class LatencyHistogram:
    """Integer-millisecond histogram with nearest-rank percentiles."""

    def __init__(self) -> None:
        self._counts = np.zeros(1024, dtype=np.int64)
        self.total = 0
        self.negative = 0
        self.max_ms: Optional[int] = None

    def add(self, latencies: Iterable[int] | np.ndarray) -> None:
        lat = np.asarray(latencies, dtype=np.int64)
        if lat.size == 0:
            return
        negative = lat < 0
        if negative.any():
            self.negative += int(negative.sum())
            lat = lat[~negative]
            if lat.size == 0:
                return
        top = int(lat.max())
        if top >= self._counts.size:
            grown = np.zeros(max(top + 1, self._counts.size * 2), dtype=np.int64)
            grown[: self._counts.size] = self._counts
            self._counts = grown
        self._counts += np.bincount(lat, minlength=self._counts.size)
        self.total += int(lat.size)
        self.max_ms = top if self.max_ms is None else max(self.max_ms, top)

    def percentile(self, p: float) -> Optional[int]:
        return percentile_from_counts(self._counts, self.total, p)


class DistinctCounter:
    """Exact count of distinct byte-string keys, stored as 128-bit digests.

    Digests are spread over 256 buckets by their first byte. Counting sorts one
    bucket at a time, so the extra memory at the end is about 1/256 of the stored
    digests instead of a second and third copy of all of them. Storage is 16 bytes
    per added key (about 0.7 GB for 45 million keys).
    """

    BUCKETS = 256

    def __init__(self) -> None:
        self._buckets: list[list[np.ndarray]] = [[] for _ in range(self.BUCKETS)]

    def add(self, keys: Iterable[bytes]) -> None:
        digests = b"".join(hashlib.blake2b(k, digest_size=16).digest() for k in keys)
        if not digests:
            return
        arr = np.frombuffer(digests, dtype="<u8").reshape(-1, 2)
        bucket = (arr[:, 0] & (self.BUCKETS - 1)).astype(np.int64)
        order = np.argsort(bucket, kind="stable")
        arr, bucket = arr[order], bucket[order]
        bounds = np.searchsorted(bucket, np.arange(self.BUCKETS + 1))
        for b in range(self.BUCKETS):
            lo, hi = int(bounds[b]), int(bounds[b + 1])
            if hi > lo:
                self._buckets[b].append(arr[lo:hi])

    def count(self) -> int:
        total = 0
        for b, chunks in enumerate(self._buckets):
            if not chunks:
                continue
            arr = np.concatenate(chunks) if len(chunks) > 1 else chunks[0]
            hi, lo = arr[:, 0], arr[:, 1]
            order = np.lexsort((lo, hi))
            hi, lo = hi[order], lo[order]
            unique = np.concatenate(([True], (hi[1:] != hi[:-1]) | (lo[1:] != lo[:-1])))
            self._buckets[b] = [np.stack([hi[unique], lo[unique]], axis=1)]
            total += int(unique.sum())
        return total


STABILITY_TOLERANCE_PCT = 15


def relative_diff_ok(a: Optional[int], b: Optional[int], tolerance_pct: int = STABILITY_TOLERANCE_PCT) -> bool:
    """True if a and b differ by at most `tolerance_pct` percent of the larger value.

    Two zeros are equal. A missing value (an empty half) is never stable.
    """
    if a is None or b is None:
        return False
    return abs(a - b) * 100 <= tolerance_pct * max(a, b)


def split_half_stable(p50_h1: Optional[int], p99_h1: Optional[int],
                      p50_h2: Optional[int], p99_h2: Optional[int]) -> bool:
    """Contract section 8: both p50 and p99 differ by at most 15 percent between halves."""
    return relative_diff_ok(p50_h1, p50_h2) and relative_diff_ok(p99_h1, p99_h2)


class OutputAccumulator:
    """Collects output records for one engine and derives the contract metrics.

    Latency and throughput use records whose `in_ts` lies in [cutoff_ms, end_ms].
    Correctness (out_records, distinct keys, missing, sink duplicates) uses the
    whole run, [correct_from_ms, correct_until_ms], so it compares against every
    unique event the generator sent. Without these two arguments both use the
    latency window.

    The latency window is also split in two equal halves by `in_ts` (first half:
    in_ts below the midpoint), each with its own histogram, for the split-half
    stability check of contract section 8.
    """

    FLUSH_AT = 65_536

    def __init__(self, cutoff_ms: int, end_ms: int,
                 correct_from_ms: int | None = None, correct_until_ms: int | None = None) -> None:
        self.cutoff_ms = int(cutoff_ms)
        self.end_ms = int(end_ms)
        self.correct_from_ms = int(cutoff_ms if correct_from_ms is None else correct_from_ms)
        self.correct_until_ms = int(end_ms if correct_until_ms is None else correct_until_ms)
        self.hist = LatencyHistogram()
        self.hist_h1 = LatencyHistogram()
        self.hist_h2 = LatencyHistogram()
        self.distinct = DistinctCounter()
        self.out_records = 0
        self.latency_records = 0
        self.excluded = 0
        self.value_errors = 0
        self._keys: list[bytes] = []
        self._lat: list[int] = []
        self._lat_h2: list[int] = []

    def _window(self, in_ts: int, out_ts: int) -> bool:
        """Record latency if `in_ts` is in the latency window; return True if it is in the correctness window."""
        in_correct = self.correct_from_ms <= in_ts <= self.correct_until_ms
        in_latency = self.cutoff_ms <= in_ts <= self.end_ms
        if not (in_correct or in_latency):
            self.excluded += 1
        if in_latency:
            self.latency_records += 1
            half = self._lat if 2 * in_ts < self.cutoff_ms + self.end_ms else self._lat_h2
            half.append(latency_ms(out_ts, in_ts))
        if in_correct:
            self.out_records += 1
        return in_correct

    def _pending(self) -> int:
        return len(self._keys)

    def _maybe_flush(self) -> None:
        if len(self._lat) + len(self._lat_h2) >= self.FLUSH_AT or self._pending() >= self.FLUSH_AT:
            self._flush()

    def offer(self, key: bytes, in_ts: int, out_ts: int, value_error: bool = False) -> None:
        if self._window(in_ts, out_ts):
            self.value_errors += int(value_error)
            self._keys.append(key)
        self._maybe_flush()

    def _flush(self) -> None:
        if self._lat:
            lat = np.asarray(self._lat, dtype=np.int64)
            self.hist.add(lat)
            self.hist_h1.add(lat)
            self._lat = []
        if self._lat_h2:
            lat = np.asarray(self._lat_h2, dtype=np.int64)
            self.hist.add(lat)
            self.hist_h2.add(lat)
            self._lat_h2 = []
        if self._keys:
            self.distinct.add(self._keys)
            self._keys = []

    def _latency_stats(self, measured_s: float) -> dict:
        hist = self.hist
        p50_h1, p99_h1 = self.hist_h1.percentile(50), self.hist_h1.percentile(99)
        p50_h2, p99_h2 = self.hist_h2.percentile(50), self.hist_h2.percentile(99)
        throughput = round(self.latency_records / measured_s, 2) if measured_s > 0 else 0.0
        return {
            "p50_ms": hist.percentile(50),
            "p95_ms": hist.percentile(95),
            "p99_ms": hist.percentile(99),
            "p999_ms": hist.percentile(99.9),
            "max_ms": hist.max_ms,
            "throughput_eps": throughput,
            "p50_ms_h1": p50_h1,
            "p99_ms_h1": p99_h1,
            "p50_ms_h2": p50_h2,
            "p99_ms_h2": p99_h2,
            "stable": split_half_stable(p50_h1, p99_h1, p50_h2, p99_h2),
            "excluded_records": self.excluded,
            "negative_latency_records": hist.negative,
        }

    def finish(self, expected_distinct: int, measured_s: float) -> dict:
        self._flush()
        distinct = self.distinct.count()
        return {
            "out_records": self.out_records,
            "distinct_out_keys": distinct,
            "expected_distinct": int(expected_distinct),
            "missing": int(expected_distinct) - distinct,
            "sink_duplicates": self.out_records - distinct,
            "value_errors": self.value_errors,
            **self._latency_stats(measured_s),
        }


# ------------------------------------------------------------ aggregate patterns

def pair_digest(key: str, window_start_ms: int) -> int:
    """64-bit digest of the output key `<key>:<window_start_ms>` (used by tools/aggregate_mismatch.py)."""
    raw = f"{key}:{int(window_start_ms)}".encode("utf-8")
    return int.from_bytes(hashlib.blake2b(raw, digest_size=8).digest(), "little")


def _digests(pair_keys: Iterable[bytes]) -> np.ndarray:
    raw = b"".join(hashlib.blake2b(k, digest_size=8).digest() for k in pair_keys)
    return np.frombuffer(raw, dtype="<u8").astype(np.uint64)


def _group_starts(sorted_digests: np.ndarray) -> np.ndarray:
    if sorted_digests.size == 0:
        return np.zeros(0, dtype=np.int64)
    flag = np.concatenate(([True], sorted_digests[1:] != sorted_digests[:-1]))
    return np.flatnonzero(flag)


class ExpectedPairs:
    """Expected (key, window) -> (count, sum_amount), sorted by 64-bit digest.

    Three parallel arrays, 24 bytes per pair, so 3 million pairs take about 72 MB.
    """

    def __init__(self, digests: np.ndarray, counts: np.ndarray, sums: np.ndarray) -> None:
        d = np.asarray(digests, dtype=np.uint64)
        c = np.asarray(counts, dtype=np.int64)
        s = np.asarray(sums, dtype=np.int64)
        order = np.argsort(d, kind="stable")
        d, c, s = d[order], c[order], s[order]
        starts = _group_starts(d)
        if starts.size != d.size:  # duplicate digests: add them up
            c, s, d = np.add.reduceat(c, starts), np.add.reduceat(s, starts), d[starts]
        self.digests, self.counts, self.sums = d, c, s

    def __len__(self) -> int:
        return int(self.digests.size)

    @classmethod
    def empty(cls) -> "ExpectedPairs":
        z = np.zeros(0, dtype=np.int64)
        return cls(np.zeros(0, dtype=np.uint64), z, z)

    @classmethod
    def merge(cls, parts: Iterable["ExpectedPairs"]) -> "ExpectedPairs":
        parts = list(parts)
        if not parts:
            return cls.empty()
        return cls(
            np.concatenate([p.digests for p in parts]),
            np.concatenate([p.counts for p in parts]),
            np.concatenate([p.sums for p in parts]),
        )

    def save(self, path) -> None:
        """Write a compressed npz atomically (temporary file, then rename)."""
        import os

        tmp = str(path) + ".tmp"
        with open(tmp, "wb") as handle:
            np.savez_compressed(handle, digest=self.digests, count=self.counts, sum_amount=self.sums)
        os.replace(tmp, path)

    @classmethod
    def load(cls, path) -> "ExpectedPairs":
        with np.load(path) as data:
            return cls(data["digest"], data["count"], data["sum_amount"])


class ExpectedBuilder:
    """Worker-side accumulator of expected pairs, kept compact with numpy."""

    FLUSH_AT = 65_536
    COMPACT_MIN = 1 << 20

    def __init__(self) -> None:
        self._keys: list[bytes] = []
        self._amounts: list[int] = []
        self._parts: list[ExpectedPairs] = []
        self._rows = 0
        self._compacted = 0

    def add(self, key: str, window_start_ms: int, amount: int) -> None:
        self._keys.append(f"{key}:{int(window_start_ms)}".encode("utf-8"))
        self._amounts.append(int(amount))
        if len(self._keys) >= self.FLUSH_AT:
            self._flush()

    def _flush(self) -> None:
        if not self._keys:
            return
        d = _digests(self._keys)
        a = np.asarray(self._amounts, dtype=np.int64)
        part = ExpectedPairs(d, np.ones(d.size, dtype=np.int64), a)
        self._keys, self._amounts = [], []
        self._parts.append(part)
        self._rows += len(part)
        if self._rows > max(self.COMPACT_MIN, 2 * self._compacted):
            merged = ExpectedPairs.merge(self._parts)
            self._parts, self._rows, self._compacted = [merged], len(merged), len(merged)

    def pairs(self) -> ExpectedPairs:
        self._flush()
        return ExpectedPairs.merge(self._parts)


class AggregateAccumulator(OutputAccumulator):
    """Collector state for update streams (06-tumbling, 07-sliding).

    Every update is (key, window) -> (count, sum_amount). Updates inside the
    correctness window are reduced to the final one per pair (highest count, then
    highest sum) as digests; latency uses the same rules as OutputAccumulator.
    """

    COMPACT_MIN = 1 << 22

    def __init__(self, cutoff_ms: int, end_ms: int,
                 correct_from_ms: int | None = None, correct_until_ms: int | None = None) -> None:
        super().__init__(cutoff_ms, end_ms, correct_from_ms, correct_until_ms)
        self._pk: list[bytes] = []
        self._pc: list[int] = []
        self._ps: list[int] = []
        self._chunks: list[tuple[np.ndarray, np.ndarray, np.ndarray]] = []
        self._chunk_rows = 0
        self._compacted_rows = 0

    def _pending(self) -> int:
        return len(self._pk)

    def offer(self, key: bytes, in_ts: int, out_ts: int, value_error: bool = False,
              count: int = 0, sum_amount: int = 0) -> None:
        """`key` is `<key>:<window_start_ms>` as bytes; count and sum come from the update."""
        if self._window(in_ts, out_ts):
            self._pk.append(key)
            self._pc.append(int(count))
            self._ps.append(int(sum_amount))
        self._maybe_flush()

    @staticmethod
    def _final(d: np.ndarray, c: np.ndarray, s: np.ndarray):
        """Reduce to one row per digest: the update with the highest (count, sum)."""
        order = np.lexsort((s, c, d))
        d, c, s = d[order], c[order], s[order]
        last = np.concatenate((d[1:] != d[:-1], [True])) if d.size else np.zeros(0, dtype=bool)
        return d[last], c[last], s[last]

    def _flush(self) -> None:
        super()._flush()
        if not self._pk:
            return
        chunk = self._final(_digests(self._pk), np.asarray(self._pc, dtype=np.int64),
                            np.asarray(self._ps, dtype=np.int64))
        self._pk, self._pc, self._ps = [], [], []
        self._chunks.append(chunk)
        self._chunk_rows += chunk[0].size
        if self._chunk_rows > max(self.COMPACT_MIN, 2 * self._compacted_rows):
            self._compact()

    def _compact(self) -> None:
        if not self._chunks:
            return
        merged = self._final(*(np.concatenate(cols) for cols in zip(*self._chunks)))
        self._chunks = [merged]
        self._chunk_rows = self._compacted_rows = int(merged[0].size)

    def finish(self, expected: ExpectedPairs, measured_s: float) -> dict:  # type: ignore[override]
        self._flush()
        self._compact()
        if self._chunks:
            sd, sc, ss = self._chunks[0]
        else:
            sd = np.zeros(0, dtype=np.uint64)
            sc = ss = np.zeros(0, dtype=np.int64)
        n_exp = len(expected)
        if n_exp:
            idx = np.minimum(np.searchsorted(expected.digests, sd), n_exp - 1)
            known = expected.digests[idx] == sd
        else:
            idx = np.zeros(sd.size, dtype=np.int64)
            known = np.zeros(sd.size, dtype=bool)
        found = int(known.sum())
        wrong = known & ((expected.counts[idx] != sc) | (expected.sums[idx] != ss)) if n_exp else known
        value_errors = int(wrong.sum()) + int((~known).sum())
        return {
            "out_records": self.out_records,
            "distinct_out_keys": int(sd.size),
            "expected_distinct": n_exp,
            "missing": n_exp - found,
            "sink_duplicates": None,
            "value_errors": value_errors,
            **self._latency_stats(measured_s),
        }

