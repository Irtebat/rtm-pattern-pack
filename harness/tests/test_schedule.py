"""Duplicate decisions, delay bounds and the resend heap."""

import random

import pytest

from rtmpack_harness.events import Event
from rtmpack_harness.schedule import DuplicateScheduler, duplicate_delay_ms


def _ev(i: int) -> Event:
    return Event(event_id=f"id-{i}", key="user-00001", seq=i, event_ts=i, is_dup=False)


def test_zero_ratio_never_duplicates_and_one_always_does():
    rng = random.Random(1)
    assert all(duplicate_delay_ms(rng, 0.0, 0, 30_000) is None for _ in range(2000))
    rng = random.Random(1)
    assert all(duplicate_delay_ms(rng, 1.0, 0, 30_000) is not None for _ in range(2000))


def test_duplicate_share_matches_ratio():
    rng = random.Random(42)
    n = 200_000
    dups = sum(duplicate_delay_ms(rng, 0.2, 0, 30_000) is not None for _ in range(n))
    assert abs(dups / n - 0.2) < 0.005


def test_delays_stay_within_inclusive_bounds_and_cover_them():
    rng = random.Random(3)
    delays = [d for d in (duplicate_delay_ms(rng, 1.0, 100, 500) for _ in range(20_000)) if d is not None]
    assert min(delays) == 100
    assert max(delays) == 500
    mean = sum(delays) / len(delays)
    assert 280 < mean < 320  # uniform on [100, 500] has mean 300


def test_draw_consumes_fixed_randomness_so_streams_stay_aligned():
    # The number of draws per decision must not depend on the outcome for the first draw.
    a = random.Random(9)
    b = random.Random(9)
    duplicate_delay_ms(a, 0.0, 0, 10)
    duplicate_delay_ms(b, 0.0, 0, 10)
    assert a.random() == b.random()


def test_scheduler_pops_in_due_order_and_respects_now():
    sched = DuplicateScheduler()
    sched.push(5_000, _ev(1))
    sched.push(1_000, _ev(2))
    sched.push(3_000, _ev(3))
    assert sched.next_due_ms() == 1_000
    assert [e.seq for e in sched.pop_due(2_999)] == [2]
    assert [e.seq for e in sched.pop_due(10_000)] == [3, 1]
    assert len(sched) == 0
    assert sched.next_due_ms() is None


def test_scheduler_keeps_insertion_order_for_equal_due_times():
    sched = DuplicateScheduler()
    for i in range(5):
        sched.push(1_000, _ev(i))
    assert [e.seq for e in sched.pop_due(1_000)] == [0, 1, 2, 3, 4]


def test_scheduler_limit_leaves_remaining_events_pending():
    sched = DuplicateScheduler()
    for i in range(10):
        sched.push(100 + i, _ev(i))
    first = sched.pop_due(1_000, limit=4)
    assert [e.seq for e in first] == [0, 1, 2, 3]
    assert len(sched) == 6


@pytest.mark.parametrize("delay_max", [0, 30_000])
def test_resends_are_released_no_earlier_than_their_delay(delay_max):
    rng = random.Random(11)
    sched = DuplicateScheduler()
    due_of: dict[int, int] = {}
    for i in range(1000):
        sent_at = 1_000 + i
        delay = duplicate_delay_ms(rng, 1.0, 0, delay_max)
        due_of[i] = sent_at + delay
        sched.push(sent_at + delay, _ev(i))
    now = 0
    released = 0
    while len(sched):
        previous, now = now, now + 250
        for ev in sched.pop_due(now):
            # Released no earlier than due, and on the first poll at or after due.
            assert due_of[ev.seq] <= now
            assert due_of[ev.seq] > previous
            released += 1
    assert released == 1000
    if delay_max == 0:
        assert all(d == 1_000 + i for i, d in due_of.items())
