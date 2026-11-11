"""Rate-controlled, multi-process load generator.

Each worker process owns one Kafka producer, a disjoint slice of the key space and
an equal share of the total rate. Workers schedule their first sends on a fixed
grid (start + i / per_worker_rate). Duplicates are resent from a heap once their
delay has elapsed. Workers start together at a common epoch, so the manifest
start time is shared by all of them.
"""

from __future__ import annotations

import datetime as dt
import math
import multiprocessing as mp
import os
import queue
import time
from dataclasses import asdict, dataclass
from typing import Callable, Optional

from rtmpack_harness.config import ConnectionConfig
from rtmpack_harness.contract import HarnessError, RunParams, input_topic, input_topics
from rtmpack_harness.events import key_range
from rtmpack_harness.patterns import Pattern, get_pattern
from rtmpack_harness.results import expected_path, manifest_path, write_manifest
from rtmpack_harness.schedule import DuplicateScheduler
from rtmpack_harness.stats import ExpectedBuilder

# Per-process target used to pick the default worker count. The actual ceiling
# must be measured on the load generator host before a run (see the README).
PER_WORKER_TARGET_EPS = 25_000
MANIFEST_VERSION = 1


def default_workers(rate_eps: int) -> int:
    return max(1, min(os.cpu_count() or 1, math.ceil(rate_eps / PER_WORKER_TARGET_EPS)))


def default_run_id(params: RunParams, now: Optional[dt.datetime] = None) -> str:
    stamp = (now or dt.datetime.now(dt.timezone.utc)).strftime("%Y%m%dT%H%M%SZ")
    return f"{params.pattern}-{stamp}-{params.rate_eps}eps"


@dataclass(frozen=True)
class WorkerSpec:
    worker_id: int
    n_workers: int
    topic: str
    rate_eps: float
    record_bytes: int
    key_cardinality: int
    dup_ratio: float
    dup_delay_ms_min: int
    dup_delay_ms_max: int
    seed: int
    start_at: float
    end_at: float
    max_batch: int = 500
    pattern: str = "01-dedup"
    # Where an aggregate pattern's worker writes its expected (key, window) side file.
    expected_file: Optional[str] = None


@dataclass
class WorkerStats:
    worker_id: int
    unique_sent: int = 0
    outputs_sent: int = 0
    dup_sent: int = 0
    follow_up_sent: int = 0
    expected_pairs: int = 0
    delivered: int = 0
    delivery_errors: int = 0
    buffer_retries: int = 0
    undelivered: int = 0
    max_lag_ms: float = 0.0

    def as_dict(self) -> dict:
        return asdict(self)


class LoadWorker:
    """Single-process sender. `clock` and `sleep` are injectable for tests."""

    def __init__(
        self,
        spec: WorkerSpec,
        producer,
        clock: Callable[[], float] = time.time,
        sleep: Callable[[float], None] = time.sleep,
    ) -> None:
        lo, hi = key_range(spec.key_cardinality, spec.n_workers, spec.worker_id)
        self.spec = spec
        self.producer = producer
        self.clock = clock
        self.sleep = sleep
        self.pattern: Pattern = get_pattern(spec.pattern)
        self.gen = self.pattern.make_generator(
            seed=spec.seed,
            worker_id=spec.worker_id,
            key_lo=lo,
            key_hi=hi,
            key_cardinality=spec.key_cardinality,
            dup_ratio=spec.dup_ratio,
            dup_delay_ms_min=spec.dup_delay_ms_min,
            dup_delay_ms_max=spec.dup_delay_ms_max,
        )
        self.sched = DuplicateScheduler()
        self.expected: Optional[ExpectedBuilder] = ExpectedBuilder() if self.pattern.is_aggregate else None
        self.interval = spec.n_workers / spec.rate_eps
        self.sent = 0
        self.stats = WorkerStats(worker_id=spec.worker_id)

    def run(self) -> WorkerStats:
        s = self.spec
        while True:
            now = self.clock()
            if now >= s.start_at:
                break
            self.sleep(min(s.start_at - now, 0.05))

        while True:
            now = self.clock()
            emitted = 0
            if now < s.end_at:
                emitted += self._emit_unique(now)
            emitted += self._emit_duplicates(now)
            self.producer.poll(0)
            if now >= s.end_at and len(self.sched) == 0:
                break
            if not emitted:
                self._idle()
        return self.stats

    def finish(self, timeout_s: float = 120.0) -> WorkerStats:
        """Flush outstanding messages and record how many were not delivered."""
        left = self.producer.flush(timeout_s)
        self.stats.undelivered = int(left or 0)
        if self.expected is not None:
            pairs = self.expected.pairs()
            self.stats.expected_pairs = len(pairs)
            if self.spec.expected_file:
                pairs.save(self.spec.expected_file)
        return self.stats

    def _emit_unique(self, now: float) -> int:
        s = self.spec
        due_upto = int((now - s.start_at) / self.interval) + 1
        n = 0
        while self.sent < due_upto and n < s.max_batch:
            scheduled = s.start_at + self.sent * self.interval
            if scheduled >= s.end_at:
                break
            lag_ms = (now - scheduled) * 1000.0
            if lag_ms > self.stats.max_lag_ms:
                self.stats.max_lag_ms = lag_ms
            event = self.gen.next_unique(int(round(scheduled * 1000)))
            self._produce(event)
            if self.expected is not None:
                for window_start in self.pattern.windows(event.event_ts):
                    self.expected.add(event.key, window_start, event.amount)
            self.sent += 1
            self.stats.unique_sent += 1
            self.stats.outputs_sent += self.pattern.outputs_per_event(event)
            delay = self.gen.draw_duplicate_delay_ms()
            if delay is not None:
                self.sched.push(int(now * 1000) + delay, event)
            n += 1
        return n

    def _emit_duplicates(self, now: float) -> int:
        due = self.sched.pop_due(int(now * 1000), limit=self.spec.max_batch * 20)
        for event in due:
            if self.pattern.follow_up is not None:
                self._produce(self.pattern.follow_up(event, int(now * 1000)))
                self.stats.follow_up_sent += 1
            else:
                self._produce(self.pattern.as_duplicate(event))
                self.stats.dup_sent += 1
        return len(due)

    def _idle(self) -> None:
        s = self.spec
        candidates = []
        next_scheduled = s.start_at + self.sent * self.interval
        candidates.append(min(next_scheduled, s.end_at))
        next_due = self.sched.next_due_ms()
        if next_due is not None:
            candidates.append(next_due / 1000.0)
        wait = min(candidates) - self.clock()
        self.sleep(min(max(wait, 1e-4), 0.002))

    def _produce(self, event) -> None:
        key = event.key.encode("utf-8")
        value = self.pattern.encode(event, self.spec.record_bytes)
        suffix = self.pattern.route(event) if self.pattern.route is not None else None
        topic = self.spec.topic if suffix is None else f"{self.spec.topic}.{suffix}"
        while True:
            try:
                self.producer.produce(topic, key=key, value=value, on_delivery=self._on_delivery)
                return
            except BufferError:
                self.stats.buffer_retries += 1
                self.producer.poll(0.05)

    def _on_delivery(self, err, msg) -> None:
        if err is None:
            self.stats.delivered += 1
        else:
            self.stats.delivery_errors += 1


def _producer_conf(conn_conf: dict[str, str]) -> dict[str, object]:
    conf: dict[str, object] = dict(conn_conf)
    conf.update(
        {
            "acks": "all",
            "enable.idempotence": True,
            "compression.type": "lz4",
            "linger.ms": 5,
            "batch.size": 1_048_576,
            "queue.buffering.max.messages": 1_000_000,
            "queue.buffering.max.kbytes": 1_048_576,
            "client.id": f"rtmpack-loadgen-{os.getpid()}",
        }
    )
    return conf


def _worker_main(spec: WorkerSpec, conn_conf: dict[str, str], out: "mp.Queue") -> None:
    from confluent_kafka import Producer

    producer = Producer(_producer_conf(conn_conf))
    worker = LoadWorker(spec, producer)
    worker.run()
    worker.finish()
    out.put(worker.stats.as_dict())


def run_loadgen(
    params: RunParams,
    conn: ConnectionConfig,
    results_dir: str,
    run_id: Optional[str] = None,
    workers: Optional[int] = None,
    startup_delay_s: float = 5.0,
    log: Callable[[str], None] = print,
) -> dict:
    """Run one load generation session and write its manifest. Returns the manifest."""
    from confluent_kafka import Producer

    params.validate()
    get_pattern(params.pattern).validate(params)
    n_workers = workers or default_workers(params.rate_eps)
    if n_workers > params.key_cardinality:
        raise HarnessError("workers cannot exceed key_cardinality")
    if n_workers > (os.cpu_count() or 1):
        log(f"warning: {n_workers} workers on {os.cpu_count()} CPUs; expect the rate to fall short")

    run_id = run_id or default_run_id(params)
    if manifest_path(results_dir, run_id).exists():
        raise HarnessError(f"run {run_id!r} already has a manifest; choose a new --run-id")

    rules = get_pattern(params.pattern)
    topic = input_topic(params.pattern)
    topics_in = input_topics(params.pattern)
    probe = Producer(_producer_conf(conn.kafka_conf()))
    for name in topics_in:
        meta = probe.list_topics(name, timeout=30)
        topic_meta = meta.topics.get(name)
        if topic_meta is None or topic_meta.error is not None:
            raise HarnessError(f"input topic {name} is missing; run `rtmpack topics create` first")
        if len(topic_meta.partitions) != params.partitions:
            log(
                f"warning: topic {name} has {len(topic_meta.partitions)} partitions, "
                f"run parameter is {params.partitions}"
            )

    start_at = time.time() + startup_delay_s
    end_at = start_at + params.duration_s
    cutoff_at = start_at + params.warmup_s
    manifest = {
        "manifest_version": MANIFEST_VERSION,
        "run_id": run_id,
        "pattern": params.pattern,
        "status": "running",
        "topics_input": topics_in,
        "rate_unit": rules.rate_unit,
        "start_at": _iso(start_at),
        "start_ms": int(round(start_at * 1000)),
        "end_at": _iso(end_at),
        "end_ms": int(round(end_at * 1000)),
        "warmup_cutoff_at": _iso(cutoff_at),
        "warmup_cutoff_ms": int(round(cutoff_at * 1000)),
        "params": params.as_dict(),
        "workers": n_workers,
        "kafka": conn.describe(),
        "counts": None,
    }
    write_manifest(results_dir, manifest)
    log(f"run {run_id}: {n_workers} workers, {params.rate_eps} eps, starts in {startup_delay_s:.0f}s")

    ctx = mp.get_context("spawn")
    out: "mp.Queue" = ctx.Queue()
    conn_conf = conn.kafka_conf()
    procs = []
    for worker_id in range(n_workers):
        spec = WorkerSpec(
            worker_id=worker_id,
            n_workers=n_workers,
            topic=topic,
            rate_eps=float(params.rate_eps),
            record_bytes=params.record_bytes,
            key_cardinality=params.key_cardinality,
            dup_ratio=params.dup_ratio,
            dup_delay_ms_min=params.dup_delay_ms_min,
            dup_delay_ms_max=params.dup_delay_ms_max,
            seed=params.seed,
            start_at=start_at,
            end_at=end_at,
            pattern=params.pattern,
            expected_file=str(expected_path(results_dir, run_id, worker_id)) if rules.is_aggregate else None,
        )
        proc = ctx.Process(target=_worker_main, args=(spec, conn_conf, out), name=f"loadgen-{worker_id}")
        proc.start()
        procs.append(proc)

    results: list[dict] = []
    try:
        while len(results) < n_workers:
            try:
                results.append(out.get(timeout=2.0))
            except queue.Empty:
                failed = [p for p in procs if not p.is_alive() and p.exitcode not in (0, None)]
                if failed:
                    raise HarnessError(f"worker {failed[0].name} exited with code {failed[0].exitcode}")
    finally:
        for proc in procs:
            proc.join(timeout=30)
            if proc.is_alive():
                proc.terminate()

    totals = {
        "unique_sent": sum(r["unique_sent"] for r in results),
        "expected_outputs": sum(r["outputs_sent"] for r in results),
        "dup_sent": sum(r["dup_sent"] for r in results),
        "follow_up_sent": sum(r["follow_up_sent"] for r in results),
        "delivered": sum(r["delivered"] for r in results),
        "delivery_errors": sum(r["delivery_errors"] for r in results),
        "undelivered": sum(r["undelivered"] for r in results),
        "buffer_retries": sum(r["buffer_retries"] for r in results),
        "max_lag_ms": round(max(r["max_lag_ms"] for r in results), 3),
        "achieved_unique_eps": round(sum(r["unique_sent"] for r in results) / params.duration_s, 2),
    }
    if rules.is_aggregate:
        # Workers own disjoint key ranges, so their (key, window) pairs are disjoint.
        totals["expected_pairs"] = sum(r["expected_pairs"] for r in results)
    if rules.manifest_counts is not None:
        totals.update(rules.manifest_counts(totals))
    manifest["status"] = "complete"
    manifest["counts"] = totals
    manifest["per_worker"] = sorted(results, key=lambda r: r["worker_id"])
    write_manifest(results_dir, manifest)
    log(
        f"run {run_id} complete: unique={totals['unique_sent']} dup={totals['dup_sent']} "
        f"errors={totals['delivery_errors']} max_lag_ms={totals['max_lag_ms']}"
    )
    return manifest


def _iso(epoch_s: float) -> str:
    return dt.datetime.fromtimestamp(epoch_s, dt.timezone.utc).isoformat(timespec="milliseconds")
