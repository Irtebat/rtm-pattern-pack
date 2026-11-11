"""Deterministic input events (contract section 2) and the shared generator base.

A generator is seeded per worker. Given the same seed, worker id, key range and
parameters, it produces the same sequence of event ids, keys, sequence numbers
and duplicate decisions. Event timestamps are supplied by the caller (the
schedule clock), so the content never depends on wall-clock timing.
"""

from __future__ import annotations

import json
import random
import uuid
from dataclasses import dataclass, replace
from typing import Callable, Optional

from rtmpack_harness.contract import HarnessError
from rtmpack_harness.schedule import duplicate_delay_ms

_PAYLOAD_TAIL = '"}'


@dataclass(frozen=True)
class Event:
    """Contract section 2 event. The last three fields are used by patterns 04 to 07 only."""

    event_id: str
    key: str
    seq: int
    event_ts: int
    is_dup: bool = False
    type: str = ""
    merchant_id: str = ""
    amount: int = 0


def key_range(key_cardinality: int, n_workers: int, worker_id: int) -> tuple[int, int]:
    """Contiguous, disjoint share of the key space for one worker: [lo, hi)."""
    if not 0 <= worker_id < n_workers:
        raise HarnessError(f"worker_id {worker_id} outside 0..{n_workers - 1}")
    if n_workers > key_cardinality:
        raise HarnessError("more workers than keys; reduce --workers or raise --key-cardinality")
    lo = worker_id * key_cardinality // n_workers
    hi = (worker_id + 1) * key_cardinality // n_workers
    return lo, hi


def pad_value(head: str, record_bytes: int) -> bytes:
    """Close a JSON document whose last field is an open `payload` string, padded to `record_bytes`."""
    pad = record_bytes - len(head) - len(_PAYLOAD_TAIL)
    if pad < 0:
        raise HarnessError(f"record_bytes={record_bytes} is smaller than the event envelope")
    return (head + "x" * pad + _PAYLOAD_TAIL).encode("ascii")


def event_encoder(extra: Callable[[Event], str] = lambda e: "") -> Callable[[Event, int], bytes]:
    """Encoder for the section 2 envelope; `extra` adds pattern fields before `payload`."""

    def encode(event: Event, record_bytes: int) -> bytes:
        head = (
            '{"event_id":"' + event.event_id + '","key":' + json.dumps(event.key)
            + f',"seq":{event.seq},"event_ts":{event.event_ts},'
            + ('"is_dup":true' if event.is_dup else '"is_dup":false')
            + extra(event) + ',"payload":"'
        )
        return pad_value(head, record_bytes)

    return encode


encode_value = event_encoder()


def as_duplicate(event: Event) -> Event:
    """A resend of `event`: same event_id, key, seq and event_ts, with is_dup set."""
    return replace(event, is_dup=True)


class KeyedGenerator:
    """Seeded random stream and key slice of one load worker.

    The seed string is `rtmpack:<pattern_id>:<seed>:<worker_id>`. 01-dedup leaves
    `pattern_id` empty and uses `rtmpack:<seed>:<worker_id>`, the form it was run
    with, so its input stays the same.
    """

    pattern_id = ""
    key_prefix = "user"

    def __init__(
        self,
        seed: int,
        worker_id: int,
        key_lo: int,
        key_hi: int,
        key_cardinality: int,
        dup_ratio: float = 0.0,
        dup_delay_ms_min: int = 0,
        dup_delay_ms_max: int = 0,
    ) -> None:
        if not 0 <= key_lo < key_hi <= key_cardinality:
            raise HarnessError(f"invalid key range [{key_lo}, {key_hi}) for cardinality {key_cardinality}")
        label = f"{self.pattern_id}:" if self.pattern_id else ""
        self._rng = random.Random(f"rtmpack:{label}{seed}:{worker_id}")
        self._lo = key_lo
        self._hi = key_hi
        self._width = max(5, len(str(key_cardinality - 1)))
        self._dup = (dup_ratio, dup_delay_ms_min, dup_delay_ms_max)

    def _draw_key(self) -> tuple[int, str]:
        """Draw a key index in this worker's slice; return (slot, key)."""
        index = self._rng.randrange(self._lo, self._hi)
        return index - self._lo, f"{self.key_prefix}-{index:0{self._width}d}"

    def _draw_uuid(self) -> str:
        return str(uuid.UUID(int=self._rng.getrandbits(128), version=4))

    def draw_duplicate_delay_ms(self) -> Optional[int]:
        """Delay before the event just generated is sent again, or None. No resends by default."""
        return None


class EventGenerator(KeyedGenerator):
    """Section 2 events with a per-key sequence number.

    Draws, in fixed order: key index, event id, then the fields from `_extra`.
    """

    def __init__(self, *args, **kwargs) -> None:
        super().__init__(*args, **kwargs)
        self._next_seq = [1] * (self._hi - self._lo)

    def _extra(self) -> dict:
        return {}

    def next_unique(self, event_ts: int) -> Event:
        slot, key = self._draw_key()
        event_id = self._draw_uuid()
        seq = self._next_seq[slot]
        self._next_seq[slot] = seq + 1
        return Event(event_id=event_id, key=key, seq=seq, event_ts=event_ts, **self._extra())


class DedupGenerator(EventGenerator):
    """01-dedup: section 2 events, each resent with probability dup_ratio."""

    def draw_duplicate_delay_ms(self) -> Optional[int]:
        # Always consumes one random number, so the stream stays aligned whatever the outcome.
        return duplicate_delay_ms(self._rng, *self._dup)
