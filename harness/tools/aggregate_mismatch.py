"""Lists the (key, window) outputs of an aggregate run whose final update is wrong.

Pass 1 reads the engine's output topic and keeps, per (key, window), the update
with the highest count, then compares it with the expected side file. Pass 2
prints every update of the mismatched pairs (partition, offset, broker time,
in_ts, count, sum), so the history of each wrong window can be read directly.

Usage (on the load generator host, from /opt/rtmpack, with the harness venv):
  venv/bin/python harness/tools/aggregate_mismatch.py --run-id <run> --engine rtm [--results-dir results]
"""
from __future__ import annotations

import argparse
import datetime as dt
import json

from confluent_kafka import Consumer, TopicPartition

from rtmpack_harness.config import add_connection_args, resolve_connection
from rtmpack_harness.contract import output_topics
from rtmpack_harness.results import load_expected, load_manifest
from rtmpack_harness.stats import pair_digest


def _scan(conn, topic: str, start_ms: int, until_ms: int, group: str):
    consumer = Consumer({**conn.kafka_conf(), "group.id": group, "enable.auto.commit": False,
                         "enable.partition.eof": True})
    parts = sorted(consumer.list_topics(topic, timeout=30).topics[topic].partitions)
    starts = consumer.offsets_for_times([TopicPartition(topic, p, start_ms) for p in parts], timeout=60)
    ends = consumer.offsets_for_times([TopicPartition(topic, p, until_ms) for p in parts], timeout=60)
    want, assign = {}, []
    for p, s, e in zip(parts, starts, ends):
        _, high = consumer.get_watermark_offsets(TopicPartition(topic, p), timeout=30, cached=False)
        so = s.offset if s.offset >= 0 else high
        eo = e.offset if e.offset >= 0 else high
        assign.append(TopicPartition(topic, p, so))
        if so < eo:
            want[p] = eo
    consumer.assign(assign)
    while want:
        for msg in consumer.consume(num_messages=10_000, timeout=5):
            if msg.error() is not None:
                want.pop(msg.partition(), None)
                continue
            if msg.offset() >= want.get(msg.partition(), -1):
                want.pop(msg.partition(), None)
                continue
            yield msg
    consumer.close()


def main() -> None:
    parser = argparse.ArgumentParser()
    parser.add_argument("--run-id", required=True)
    parser.add_argument("--engine", required=True, choices=["rtm", "flink"])
    parser.add_argument("--results-dir", default="results")
    parser.add_argument("--grace-s", type=int, default=90)
    add_connection_args(parser)
    args = parser.parse_args()
    conn = resolve_connection(args)

    manifest = load_manifest(args.results_dir, args.run_id)
    expected = load_expected(args.results_dir, args.run_id)
    exp = dict(zip(expected.digests.tolist(), zip(expected.counts.tolist(), expected.sums.tolist())))
    start_ms, until_ms = int(manifest["start_ms"]), int(manifest["end_ms"]) + args.grace_s * 1000

    final: dict[int, tuple[int, int, str, int]] = {}
    for topic in output_topics(manifest["pattern"], args.engine):
        for msg in _scan(conn, topic, start_ms, until_ms, f"rtmpack-mismatch-{args.run_id}-1"):
            v = json.loads(msg.value())
            d = pair_digest(v["key"], int(v["window_start_ms"]))
            if d not in final or int(v["count"]) > final[d][0]:
                final[d] = (int(v["count"]), int(v["sum_amount"]), v["key"], int(v["window_start_ms"]))

    bad = {d: f for d, f in final.items() if exp.get(d) != (f[0], f[1])}
    print(f"{args.engine}: {len(final)} pairs seen, {len(bad)} with a wrong final update")
    for d, (c, s, key, ws) in sorted(bad.items(), key=lambda x: x[1][3]):
        ec, es = exp.get(d, (0, 0))
        wsd = dt.datetime.fromtimestamp(ws / 1000, dt.timezone.utc).strftime("%H:%M:%S")
        print(f"pair {key}:{ws} (window {wsd} UTC): final count {c} sum {s}; expected count {ec} sum {es}")
    if not bad:
        return

    print("updates of the mismatched pairs (partition offset out_ts in_ts count sum):")
    for topic in output_topics(manifest["pattern"], args.engine):
        for msg in _scan(conn, topic, start_ms, until_ms, f"rtmpack-mismatch-{args.run_id}-2"):
            v = json.loads(msg.value())
            d = pair_digest(v["key"], int(v["window_start_ms"]))
            if d in bad:
                out_ts = msg.timestamp()[1]
                print(f"  {v['key']}:{v['window_start_ms']} p{msg.partition()} o{msg.offset()} "
                      f"out {out_ts} in {v['in_ts']} lat {out_ts - int(v['in_ts'])} ms "
                      f"count {v['count']} sum {v['sum_amount']}")


if __name__ == "__main__":
    main()
