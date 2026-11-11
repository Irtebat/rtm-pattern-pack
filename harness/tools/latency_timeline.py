"""Per-second latency timeline for one engine's output in a finished run.

Prints the seconds with the highest maximum latency and their position within
the minute, which shows whether tail latency follows a periodic cause such as
batch or checkpoint boundaries.

Usage (on the load generator host, from /opt/rtmpack, with the harness venv):
  venv/bin/python harness/tools/latency_timeline.py --run-id 01-dedup-tuned-r2000-20261108T120000Z --engine rtm [--results-dir results] [--csv out.csv]
Connection settings come from the same RTMPACK_* environment variables as the CLI.
"""
from __future__ import annotations

import argparse
import json
from collections import defaultdict

import numpy as np
from confluent_kafka import Consumer, TopicPartition

from rtmpack_harness.config import add_connection_args, resolve_connection
from rtmpack_harness.contract import output_topics
from rtmpack_harness.results import load_manifest


def main() -> None:
    parser = argparse.ArgumentParser()
    parser.add_argument("--run-id", required=True)
    parser.add_argument("--engine", required=True, choices=["rtm", "flink"])
    parser.add_argument("--results-dir", default="results")
    parser.add_argument("--top", type=int, default=15)
    parser.add_argument("--csv", help="Also write every measured second to this CSV file.")
    add_connection_args(parser)
    args = parser.parse_args()
    conn = resolve_connection(args)

    manifest = load_manifest(args.results_dir, args.run_id)
    cutoff_ms, end_ms = int(manifest["warmup_cutoff_ms"]), int(manifest["end_ms"])
    topics = output_topics(manifest["pattern"], args.engine)  # every output topic of the engine

    consumer = Consumer({**conn.kafka_conf(), "group.id": f"rtmpack-timeline-{args.run_id}",
                         "enable.auto.commit": False, "enable.partition.eof": True})
    want = {}
    assign = []
    for topic in topics:
        parts = sorted(consumer.list_topics(topic, timeout=30).topics[topic].partitions)
        starts = consumer.offsets_for_times([TopicPartition(topic, p, cutoff_ms) for p in parts], timeout=60)
        ends = consumer.offsets_for_times([TopicPartition(topic, p, end_ms + 60_000) for p in parts], timeout=60)
        for p, s, e in zip(parts, starts, ends):
            _, high = consumer.get_watermark_offsets(TopicPartition(topic, p), timeout=30, cached=False)
            so = s.offset if s.offset >= 0 else high
            eo = e.offset if e.offset >= 0 else high
            assign.append(TopicPartition(topic, p, so))
            if so < eo:
                want[(topic, p)] = eo
    consumer.assign(assign)

    per_second: dict[int, list[int]] = defaultdict(list)
    while want:
        for msg in consumer.consume(num_messages=10_000, timeout=5):
            tp = (msg.topic(), msg.partition())
            if msg.error() is not None:
                want.pop(tp, None)
                continue
            if msg.offset() >= want.get(tp, -1):
                want.pop(tp, None)
                continue
            in_ts = int(json.loads(msg.value())["in_ts"])
            if cutoff_ms <= in_ts <= end_ms:
                per_second[(in_ts - cutoff_ms) // 1000].append(msg.timestamp()[1] - in_ts)
    consumer.close()

    rows = []
    for sec, lat in sorted(per_second.items()):
        a = np.asarray(lat)
        rows.append((sec, len(a), int(np.percentile(a, 50)), int(np.percentile(a, 99)), int(a.max())))
    if args.csv:
        with open(args.csv, "w") as fh:
            fh.write("second,epoch_s,count,p50_ms,p99_ms,max_ms\n")
            for sec, n, p50, p99, mx in rows:
                fh.write(f"{sec},{(cutoff_ms // 1000) + sec},{n},{p50},{p99},{mx}\n")
    print(f"{args.engine}: {len(rows)} seconds measured")
    print("second  wall_sec_of_minute  count  p50  p99  max")
    for sec, n, p50, p99, mx in sorted(rows, key=lambda r: -r[4])[: args.top]:
        wall = ((cutoff_ms // 1000) + sec) % 60
        print(f"{sec:6d}  {wall:18d}  {n:5d}  {p50:4d}  {p99:4d}  {mx:5d}")
    spikes = [r[0] for r in rows if r[4] > 250]
    if len(spikes) > 1:
        gaps = np.diff(spikes)
        print(f"seconds with max > 250 ms: {len(spikes)}; median gap between them: {int(np.median(gaps))} s")


if __name__ == "__main__":
    main()
