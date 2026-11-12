"""Shared by the window patterns 06-tumbling and 07-sliding: events with an `amount` field."""

from __future__ import annotations

from rtmpack_harness.events import EventGenerator, event_encoder
from rtmpack_harness.patterns.base import AMOUNT_MAX, AMOUNT_MIN, EXTENDED_MIN_RECORD_BYTES, Pattern, aggregate_key


class AmountGenerator(EventGenerator):
    def _extra(self) -> dict:
        return {"amount": self._rng.randint(AMOUNT_MIN, AMOUNT_MAX)}


def window_pattern(pid: str, description: str, generator, windows) -> Pattern:
    """A count and sum per (key, window) pattern; `windows(event_ts)` gives the window starts."""
    return Pattern(
        id=pid,
        description=description,
        make_generator=generator,
        encode=event_encoder(lambda e: f',"amount":{e.amount}'),
        output_keys=lambda event: [aggregate_key(event.key, w) for w in windows(event.event_ts)],
        expected_count_field="expected_pairs",
        min_record_bytes=EXTENDED_MIN_RECORD_BYTES,
        kind="aggregate",
        windows=windows,
    )
