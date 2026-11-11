"""Generator determinism, key partitioning and record padding."""

import json
import random

import pytest

from rtmpack_harness.contract import HarnessError
from rtmpack_harness.events import (
    Event,
    EventGenerator,
    as_duplicate,
    encode_value,
    key_range,
)


def _generator(seed=42, worker_id=0, lo=0, hi=1000, card=1000, dup_ratio=0.2):
    return EventGenerator(
        seed=seed,
        worker_id=worker_id,
        key_lo=lo,
        key_hi=hi,
        key_cardinality=card,
        dup_ratio=dup_ratio,
        dup_delay_ms_min=0,
        dup_delay_ms_max=30_000,
    )


def _stream(gen, n, ts_start=1_700_000_000_000):
    out = []
    for i in range(n):
        ev = gen.next_unique(ts_start + i)
        delay = gen.draw_duplicate_delay_ms()
        out.append((ev, delay))
    return out


def test_same_seed_gives_identical_stream():
    a = _stream(_generator(seed=7), 500)
    b = _stream(_generator(seed=7), 500)
    assert a == b
    assert [encode_value(e, 1024) for e, _ in a] == [encode_value(e, 1024) for e, _ in b]


def test_different_seed_or_worker_changes_stream():
    base = [e.event_id for e, _ in _stream(_generator(seed=7), 50)]
    other_seed = [e.event_id for e, _ in _stream(_generator(seed=8), 50)]
    other_worker = [e.event_id for e, _ in _stream(_generator(seed=7, worker_id=1), 50)]
    assert base != other_seed
    assert base != other_worker


def test_keys_stay_in_range_and_sequence_numbers_are_per_key():
    gen = _generator(lo=200, hi=260, card=1000)
    seen_seq: dict[str, list[int]] = {}
    for ev, _ in _stream(gen, 5000):
        index = int(ev.key.split("-")[1])
        assert 200 <= index < 260
        seen_seq.setdefault(ev.key, []).append(ev.seq)
    for key, seqs in seen_seq.items():
        assert seqs == list(range(1, len(seqs) + 1)), key


def test_event_ids_are_unique_uuid4_strings():
    events = [e for e, _ in _stream(_generator(), 2000)]
    ids = [e.event_id for e in events]
    assert len(set(ids)) == len(ids)
    assert all(len(i) == 36 and i[14] == "4" for i in ids)


def test_key_format_uses_fixed_width():
    ev = _generator(card=100_000).next_unique(0)
    assert len(ev.key) == len("user-00000")
    assert ev.key.startswith("user-")


def test_duplicate_reuses_identity_and_sets_flag():
    original = _generator().next_unique(123)
    dup = as_duplicate(original)
    assert dup.is_dup is True
    assert original.is_dup is False
    assert (dup.event_id, dup.key, dup.seq, dup.event_ts) == (
        original.event_id,
        original.key,
        original.seq,
        original.event_ts,
    )


def test_key_ranges_are_disjoint_and_cover_the_space():
    card, workers = 100_000, 7
    ranges = [key_range(card, workers, w) for w in range(workers)]
    assert ranges[0][0] == 0
    assert ranges[-1][1] == card
    for (lo1, hi1), (lo2, _) in zip(ranges, ranges[1:]):
        assert hi1 == lo2
    assert sum(hi - lo for lo, hi in ranges) == card


def test_key_range_rejects_more_workers_than_keys():
    with pytest.raises(HarnessError):
        key_range(3, 4, 0)


@pytest.mark.parametrize("record_bytes", [160, 256, 1024, 4096])
def test_encoded_value_is_exactly_record_bytes(record_bytes):
    gen = _generator(card=100_000, lo=0, hi=100_000)
    for i in range(300):
        ev = gen.next_unique(1_700_000_000_000 + i * 997)
        for variant in (ev, as_duplicate(ev)):
            value = encode_value(variant, record_bytes)
            assert len(value) == record_bytes


def test_encoded_value_is_valid_json_with_contract_fields():
    ev = Event(
        event_id="6f1f1d4e-2a8b-4c3d-9e7f-0a1b2c3d4e5f",
        key="user-00042",
        seq=17,
        event_ts=1_700_000_000_123,
        is_dup=True,
    )
    doc = json.loads(encode_value(ev, 1024))
    assert set(doc) == {"event_id", "key", "seq", "event_ts", "is_dup", "payload"}
    assert doc["event_id"] == ev.event_id
    assert doc["key"] == "user-00042"
    assert doc["seq"] == 17
    assert doc["event_ts"] == 1_700_000_000_123
    assert doc["is_dup"] is True
    assert len(encode_value(ev, 1024)) == 1024
    assert doc["payload"] == "x" * len(doc["payload"])


def test_record_too_small_for_envelope_is_rejected():
    ev = _generator().next_unique(0)
    with pytest.raises(HarnessError):
        encode_value(ev, 50)


def test_random_module_is_not_used_globally():
    # Generators must not depend on the global random state.
    random.seed(1)
    a = _stream(_generator(seed=3), 20)
    random.seed(999)
    b = _stream(_generator(seed=3), 20)
    assert a == b
