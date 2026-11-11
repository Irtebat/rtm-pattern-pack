"""Load worker behaviour with a fake clock and a fake producer (no broker needed)."""

import json

from rtmpack_harness.events import key_range
from rtmpack_harness.loadgen import LoadWorker, WorkerSpec


class FakeClock:
    def __init__(self, t: float = 1_000.0) -> None:
        self.t = t

    def __call__(self) -> float:
        return self.t

    def sleep(self, seconds: float) -> None:
        self.t += seconds


class FakeProducer:
    def __init__(self) -> None:
        self.records: list[dict] = []

    def produce(self, topic, key, value, on_delivery=None):
        self.records.append({"topic": topic, "key": key, "value": value})
        if on_delivery is not None:
            on_delivery(None, None)

    def poll(self, timeout=0):
        return 0

    def flush(self, timeout=0):
        return 0


def _spec(**overrides) -> WorkerSpec:
    base = dict(
        worker_id=0,
        n_workers=1,
        topic="rtmpack.01-dedup.input",
        rate_eps=200.0,
        record_bytes=256,
        key_cardinality=1000,
        dup_ratio=0.5,
        dup_delay_ms_min=0,
        dup_delay_ms_max=500,
        seed=42,
        start_at=1_000.0,
        end_at=1_002.0,
    )
    base.update(overrides)
    return WorkerSpec(**base)


def _run(spec: WorkerSpec):
    clock = FakeClock(spec.start_at - 0.5)
    producer = FakeProducer()
    worker = LoadWorker(spec, producer, clock=clock, sleep=clock.sleep)
    worker.run()
    worker.finish()
    return worker, producer


def _decode(record):
    return json.loads(record["value"])


def test_unique_sends_follow_the_rate_grid_exactly():
    worker, producer = _run(_spec(dup_ratio=0.0))
    assert worker.stats.unique_sent == 400  # 200 eps for 2 s
    assert worker.stats.dup_sent == 0
    assert len(producer.records) == 400
    docs = [_decode(r) for r in producer.records]
    assert len({d["event_id"] for d in docs}) == 400
    assert all(d["is_dup"] is False for d in docs)
    assert all(len(r["value"]) == 256 for r in producer.records)


def test_duplicates_resend_earlier_events_with_same_identity():
    worker, producer = _run(_spec(dup_ratio=0.5))
    docs = [_decode(r) for r in producer.records]
    firsts = {}
    dup_count = 0
    for doc in docs:
        if doc["is_dup"]:
            dup_count += 1
            original = firsts[doc["event_id"]]
            assert doc["key"] == original["key"]
            assert doc["seq"] == original["seq"]
            assert doc["event_ts"] == original["event_ts"]
        else:
            assert doc["event_id"] not in firsts
            firsts[doc["event_id"]] = doc
    assert dup_count == worker.stats.dup_sent
    assert 0.35 * worker.stats.unique_sent < dup_count < 0.65 * worker.stats.unique_sent


def test_duplicates_are_flushed_after_the_run_end():
    worker, producer = _run(_spec(dup_ratio=1.0, dup_delay_ms_min=400, dup_delay_ms_max=500))
    assert worker.stats.dup_sent == worker.stats.unique_sent
    assert len(worker.sched) == 0


def test_identical_specs_produce_identical_payload_sequences():
    _, p1 = _run(_spec(seed=5))
    _, p2 = _run(_spec(seed=5))
    assert [r["value"] for r in p1.records] == [r["value"] for r in p2.records]
    assert [r["key"] for r in p1.records] == [r["key"] for r in p2.records]


def test_worker_keys_stay_inside_its_share():
    card, n = 1000, 4
    for worker_id in range(n):
        lo, hi = key_range(card, n, worker_id)
        worker, producer = _run(_spec(worker_id=worker_id, n_workers=n, key_cardinality=card, rate_eps=400.0))
        for record in producer.records:
            index = int(record["key"].decode().split("-")[1])
            assert lo <= index < hi
