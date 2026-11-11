"""Duplicate scheduling.

A duplicate is a resend of an already-sent event after a delay drawn uniformly
from [dup_delay_ms_min, dup_delay_ms_max]. Pending resends wait in a min-heap
ordered by due time.
"""

from __future__ import annotations

import heapq
import itertools
import random
from typing import TYPE_CHECKING, Optional

if TYPE_CHECKING:  # pragma: no cover
    from rtmpack_harness.events import Event


def duplicate_delay_ms(
    rng: random.Random,
    dup_ratio: float,
    delay_min_ms: int,
    delay_max_ms: int,
) -> Optional[int]:
    """Return the resend delay for one event, or None when it is not duplicated.

    Each event is resent with probability `dup_ratio`. The function always consumes
    one random number, so the random stream stays aligned whatever the outcome.
    """
    if rng.random() >= dup_ratio:
        return None
    return rng.randint(delay_min_ms, delay_max_ms)


class DuplicateScheduler:
    """Min-heap of pending resends keyed by due time in epoch milliseconds."""

    def __init__(self) -> None:
        self._heap: list[tuple[int, int, "Event"]] = []
        self._tiebreak = itertools.count()

    def push(self, due_ms: int, event: "Event") -> None:
        heapq.heappush(self._heap, (int(due_ms), next(self._tiebreak), event))

    def pop_due(self, now_ms: int, limit: Optional[int] = None) -> list["Event"]:
        """Remove and return events due at or before `now_ms`, in due-time order."""
        due: list["Event"] = []
        while self._heap and self._heap[0][0] <= now_ms:
            if limit is not None and len(due) >= limit:
                break
            due.append(heapq.heappop(self._heap)[2])
        return due

    def next_due_ms(self) -> Optional[int]:
        return self._heap[0][0] if self._heap else None

    def __len__(self) -> int:
        return len(self._heap)
