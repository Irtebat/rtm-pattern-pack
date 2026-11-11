"""Turns output messages into accumulator calls, one pattern at a time.

Kept apart from collect.py so the multi-topic, value-check and aggregate logic
can be tested without a broker.
"""

from __future__ import annotations

from typing import Optional

from rtmpack_harness.patterns import Pattern, aggregate_key
from rtmpack_harness.stats import AggregateAccumulator, ExpectedPairs, OutputAccumulator


def make_accumulator(rules: Pattern, cutoff_ms: int, end_ms: int, correct_from_ms: int, correct_until_ms: int):
    cls = AggregateAccumulator if rules.is_aggregate else OutputAccumulator
    return cls(cutoff_ms, end_ms, correct_from_ms=correct_from_ms, correct_until_ms=correct_until_ms)


class OutputIngest:
    """Feeds decoded output records to the accumulator.

    `offer` returns False for a malformed record (missing key, `in_ts` or, for
    aggregates, missing aggregate fields), which the caller counts separately.
    """

    def __init__(self, rules: Pattern, acc, multi_topic: bool = False) -> None:
        self.rules = rules
        self.acc = acc
        self.multi_topic = multi_topic

    def offer(self, topic: str, suffix: Optional[str], key: Optional[bytes], doc: dict, out_ts: int) -> bool:
        try:
            in_ts = int(doc["in_ts"])
        except (TypeError, ValueError, KeyError):
            return False
        rules = self.rules
        if rules.is_aggregate:
            try:
                count = int(doc["count"])
                total = int(doc["sum_amount"])
                pair = aggregate_key(str(doc["key"]), int(doc["window_start_ms"]))
            except (TypeError, ValueError, KeyError):
                return False
            self.acc.offer(pair, in_ts, out_ts, count=count, sum_amount=total)
            return True
        if key is None:
            return False
        error = rules.check_value is not None and not rules.check_value(suffix, doc)
        # With several output topics a key counts once per topic.
        acc_key = topic.encode("utf-8") + b"\x00" + key if self.multi_topic else key
        self.acc.offer(acc_key, in_ts, out_ts, value_error=bool(error))
        return True

    def finish(self, manifest: dict, measured_s: float, expected: Optional[ExpectedPairs] = None) -> dict:
        if self.rules.is_aggregate:
            if expected is None:
                raise ValueError("aggregate patterns need the expected pairs")
            return self.acc.finish(expected, measured_s)
        return self.acc.finish(self.rules.expected_distinct(manifest), measured_s)
