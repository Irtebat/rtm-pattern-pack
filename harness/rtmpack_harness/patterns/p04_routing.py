"""04-routing: write each event to a topic per event type and to an all-events topic."""

from __future__ import annotations

import json
from typing import Optional

from rtmpack_harness.events import EventGenerator, event_encoder
from rtmpack_harness.patterns.base import EXTENDED_MIN_RECORD_BYTES, Pattern

ROUTING_TYPES = ("payment", "order", "click", "login")
ROUTING_WEIGHTS = (0.4, 0.3, 0.2, 0.1)


class RoutingGenerator(EventGenerator):
    pattern_id = "04-routing"

    def _extra(self) -> dict:
        u = self._rng.random()
        acc = 0.0
        for name, weight in zip(ROUTING_TYPES, ROUTING_WEIGHTS):
            acc += weight
            if u < acc:
                return {"type": name}
        return {"type": ROUTING_TYPES[-1]}


def _routing_output_keys(event) -> list[bytes]:
    # One record in the type topic and one in the all topic; both keyed by event_id.
    return [event.event_id.encode("utf-8")] * 2


def _routing_check(suffix: Optional[str], doc: dict) -> bool:
    if suffix is None or suffix == "all":
        return True
    return doc.get("type") == suffix


PATTERN = Pattern(
    id="04-routing",
    description="demux by type and fan-out to an all-events topic",
    make_generator=RoutingGenerator,
    encode=event_encoder(lambda e: ',"type":' + json.dumps(e.type)),
    output_keys=_routing_output_keys,
    expected_count_field="expected_outputs",
    min_record_bytes=EXTENDED_MIN_RECORD_BYTES,
    check_value=_routing_check,
    output_suffixes=ROUTING_TYPES + ("all",),
)
