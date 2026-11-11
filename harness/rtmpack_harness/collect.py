"""Collect one engine's output for a run and write its results record."""

from __future__ import annotations

import json
import math
import uuid
from typing import Callable, Optional

from confluent_kafka import TIMESTAMP_LOG_APPEND_TIME, Consumer, KafkaError, TopicPartition

from rtmpack_harness.config import ConnectionConfig
from rtmpack_harness.contract import (
    RESULT_FIELDS,
    HarnessError,
    check_engine,
    output_topic,
    output_topics,
    topic_suffix,
)
from rtmpack_harness.patterns import get_pattern
from rtmpack_harness.ingest import OutputIngest, make_accumulator
from rtmpack_harness.results import load_expected, load_manifest, write_result


def default_grace_s(dup_delay_ms_max: int) -> int:
    """Time after run end to keep reading: the longest resend delay plus a margin."""
    return math.ceil(dup_delay_ms_max / 1000) + 60


def collect_run(
    conn: ConnectionConfig,
    results_dir: str,
    run_id: str,
    engine: str,
    engine_version: str,
    compute_desc: str,
    vcpu: int,
    cost_per_hour_usd: float,
    grace_s: Optional[int] = None,
    idle_timeout_s: float = 120.0,
    log: Callable[[str], None] = print,
) -> tuple[dict, dict]:
    """Read the output topic for the run window and write results/<run_id>.jsonl.

    Returns (record, diagnostics). The record follows contract section 6; the
    diagnostics (excluded and malformed counts) are printed, not written.
    """
    check_engine(engine)
    manifest = load_manifest(results_dir, run_id)
    if manifest.get("status") != "complete":
        raise HarnessError(f"run {run_id!r} is not complete (status {manifest.get('status')!r})")

    pattern = manifest["pattern"]
    rules = get_pattern(pattern)
    params = manifest["params"]
    base_topic = output_topic(pattern, engine)
    topics = output_topics(pattern, engine)
    expected = load_expected(results_dir, run_id) if rules.is_aggregate else None
    start_ms = int(manifest["start_ms"])
    cutoff_ms = int(manifest["warmup_cutoff_ms"])
    end_ms = int(manifest["end_ms"])
    if grace_s is None:
        grace_s = default_grace_s(params["dup_delay_ms_max"])
    read_until_ms = end_ms + int(grace_s) * 1000

    consumer = Consumer(
        {
            **conn.kafka_conf(),
            "group.id": f"rtmpack-collect-{run_id}-{engine}-{uuid.uuid4().hex[:8]}",
            "enable.auto.commit": False,
            "auto.offset.reset": "earliest",
            "enable.partition.eof": True,
        }
    )
    try:
        assignments: list[TopicPartition] = []
        wanted_end: dict[tuple[str, int], int] = {}
        for topic in topics:
            topic_meta = consumer.list_topics(topic, timeout=30).topics.get(topic)
            if topic_meta is None or topic_meta.error is not None:
                raise HarnessError(f"output topic {topic} does not exist")
            partitions = sorted(topic_meta.partitions)

            starts = consumer.offsets_for_times([TopicPartition(topic, p, start_ms) for p in partitions], timeout=60)
            ends = consumer.offsets_for_times([TopicPartition(topic, p, read_until_ms) for p in partitions], timeout=60)
            for p, start_tp, end_tp in zip(partitions, starts, ends):
                low, high = consumer.get_watermark_offsets(TopicPartition(topic, p), timeout=30, cached=False)
                start_off = start_tp.offset if start_tp.offset >= 0 else high
                end_off = end_tp.offset if end_tp.offset >= 0 else high
                assignments.append(TopicPartition(topic, p, start_off))
                if start_off < end_off:
                    wanted_end[(topic, p)] = end_off
        consumer.assign(assignments)

        # Correctness covers the whole run: every event sent between start and end,
        # matched against all its outputs (duplicates arrive up to dup_delay_ms_max later,
        # but carry the in_ts of their own append, which is still before end_ms + delay).
        acc = make_accumulator(rules, cutoff_ms, end_ms, start_ms, end_ms + int(params["dup_delay_ms_max"]))
        ingest = OutputIngest(rules, acc, multi_topic=len(topics) > 1)
        pending = dict(wanted_end)
        malformed = 0
        non_append_time = 0
        messages = 0
        idle = 0.0
        while pending:
            batch = consumer.consume(num_messages=5000, timeout=1.0)
            if not batch:
                idle += 1.0
                if idle >= idle_timeout_s:
                    log(f"warning: no output for {idle_timeout_s:.0f}s; stopping with {len(pending)} partitions unread")
                    break
                continue
            idle = 0.0
            for msg in batch:
                tp = (msg.topic(), msg.partition())
                if msg.error() is not None:
                    if msg.error().code() == KafkaError._PARTITION_EOF:
                        pending.pop(tp, None)
                        continue
                    raise HarnessError(str(msg.error()))
                if tp not in pending:
                    continue
                offset = msg.offset()
                if offset >= wanted_end[tp]:
                    pending.pop(tp, None)
                    continue
                messages += 1
                ts_type, out_ts = msg.timestamp()
                if ts_type != TIMESTAMP_LOG_APPEND_TIME:
                    non_append_time += 1
                else:
                    try:
                        doc = json.loads(msg.value())
                        ok = isinstance(doc, dict) and ingest.offer(
                            msg.topic(), topic_suffix(msg.topic(), base_topic), msg.key(), doc, int(out_ts)
                        )
                    except (TypeError, ValueError):
                        ok = False
                    if not ok:
                        malformed += 1
                if offset + 1 >= wanted_end[tp]:
                    pending.pop(tp, None)
    finally:
        consumer.close()

    measured_s = (end_ms - cutoff_ms) / 1000.0
    stats = ingest.finish(manifest, measured_s, expected)
    run_fields = {
        "run_id": run_id,
        "pattern": pattern,
        "engine": engine,
        "engine_version": engine_version,
        "started_at": manifest["start_at"],
        "rate_eps": params["rate_eps"],
        "record_bytes": params["record_bytes"],
        "partitions": params["partitions"],
        "compute_desc": compute_desc,
        "vcpu": int(vcpu),
        "cost_per_hour_usd": float(cost_per_hour_usd),
    }
    # Every other contract field comes from the accumulator.
    record = {f: run_fields[f] if f in run_fields else stats[f] for f in RESULT_FIELDS}
    diagnostics = {
        "topics": topics,
        "messages_read": messages,
        "excluded_outside_window": stats["excluded_records"],
        "negative_latency_records": stats["negative_latency_records"],
        "malformed_records": malformed,
        "non_append_time_records": non_append_time,
        "read_until_ms": read_until_ms,
    }
    write_result(results_dir, run_id, record)
    return record, diagnostics
