"""Local tests for the 04-routing RTM transformation. Runs the pure function in batch mode."""

import datetime as dt
import json
import sys
from collections import Counter
from pathlib import Path

import pytest

sys.path.insert(0, str(Path(__file__).resolve().parents[1]))

from pyspark.sql import SparkSession  # noqa: E402
from pyspark.sql.types import BinaryType, StructField, StructType, TimestampType  # noqa: E402

import routing_rtm  # noqa: E402

KAFKA_SCHEMA = StructType(
    [
        StructField("key", BinaryType(), True),
        StructField("value", BinaryType(), True),
        StructField("timestamp", TimestampType(), True),
    ]
)

BASE = "rtmpack.04-routing.out.rtm"
T0 = dt.datetime(2026, 1, 1, 0, 0, 0)


@pytest.fixture(scope="module")
def spark():
    session = (
        SparkSession.builder.master("local[2]")
        .appName("test-routing-rtm")
        .config("spark.sql.shuffle.partitions", "2")
        .config("spark.ui.enabled", "false")
        .getOrCreate()
    )
    yield session
    session.stop()


def _row(event_id, etype, kafka_ts=T0, seq=7, event_ts=1767225600000, raw_value=None):
    value = raw_value
    if value is None:
        value = json.dumps(
            {
                "event_id": event_id,
                "key": "user-00042",
                "seq": seq,
                "event_ts": event_ts,
                "is_dup": False,
                "type": etype,
                "payload": "x" * 16,
            }
        )
    return (event_id.encode("utf-8"), value.encode("utf-8"), kafka_ts)


def _run(spark, rows):
    raw = spark.createDataFrame(rows, KAFKA_SCHEMA)
    return routing_rtm.route_events(raw, BASE).collect()


def test_each_event_becomes_two_rows_type_topic_and_all_topic(spark):
    out = _run(spark, [_row("e-1", "payment")])

    assert sorted(r["topic"] for r in out) == [f"{BASE}.all", f"{BASE}.payment"]
    assert {r["key"] for r in out} == {"e-1"}


def test_all_four_types_route_to_their_own_topic(spark):
    types = list(routing_rtm.EVENT_TYPES)
    out = _run(spark, [_row(f"e-{t}", t) for t in types])

    assert len(out) == 2 * len(types)
    counts = Counter(r["topic"] for r in out)
    for t in types:
        assert counts[f"{BASE}.{t}"] == 1
    assert counts[f"{BASE}.all"] == len(types)
    for r in out:
        body = json.loads(r["value"])
        if r["topic"] != f"{BASE}.all":
            assert r["topic"] == f"{BASE}.{body['type']}"


def test_output_json_has_contract_fields_and_no_payload(spark):
    out = _run(spark, [_row("e-9", "click", seq=11, event_ts=1767225600123)])

    for r in out:
        body = json.loads(r["value"])
        assert set(body) == {"event_id", "key", "seq", "event_ts", "is_dup", "type", "in_ts", "engine"}
        assert body["event_id"] == "e-9"
        assert body["key"] == "user-00042"
        assert body["seq"] == 11
        assert body["event_ts"] == 1767225600123
        assert body["is_dup"] is False
        assert body["type"] == "click"
        assert body["engine"] == "rtm"
    # Both copies carry identical values.
    assert len({r["value"] for r in out}) == 1


def test_in_ts_is_kafka_timestamp_in_epoch_millis_for_both_rows(spark):
    kafka_ts = dt.datetime(2026, 1, 1, 0, 0, 1, 250000, tzinfo=dt.timezone.utc)
    out = _run(spark, [_row("e-1", "login", kafka_ts=kafka_ts)])

    expected = int(kafka_ts.timestamp() * 1000)
    assert [json.loads(r["value"])["in_ts"] for r in out] == [expected] * 2


def test_total_output_is_twice_the_events_with_distinct_topic_and_key(spark):
    types = list(routing_rtm.EVENT_TYPES)
    rows = [_row(f"e-{i}", types[i % 4]) for i in range(40)]

    out = _run(spark, rows)

    assert len(out) == 80
    assert len({(r["topic"], r["key"]) for r in out}) == 80


def test_unknown_type_goes_to_all_topic_only(spark):
    out = _run(spark, [_row("e-1", "refund"), _row("e-2", None)])

    assert sorted((r["topic"], r["key"]) for r in out) == [(f"{BASE}.all", "e-1"), (f"{BASE}.all", "e-2")]


def test_malformed_input_produces_no_output(spark):
    out = _run(spark, [_row("e-bad", "x", raw_value="not json"), _row("e-ok", "order")])

    assert {r["key"] for r in out} == {"e-ok"}


def test_output_base_is_used_as_given(spark):
    raw = spark.createDataFrame([_row("e-1", "order")], KAFKA_SCHEMA)
    out = routing_rtm.route_events(raw, "custom.base").collect()

    assert sorted(r["topic"] for r in out) == ["custom.base.all", "custom.base.order"]


def test_kafka_sasl_options_use_shaded_scram_module():
    opts = routing_rtm.kafka_sasl_options("b:9096", "u", 'p"w')

    assert opts["kafka.security.protocol"] == "SASL_SSL"
    assert opts["kafka.sasl.mechanism"] == "SCRAM-SHA-512"
    assert opts["kafka.sasl.jaas.config"].startswith(routing_rtm.SCRAM_LOGIN_MODULE)
    assert 'password="p\\"w"' in opts["kafka.sasl.jaas.config"]


def test_parse_args_defaults():
    args = routing_rtm.parse_args(["--checkpoint-location", "/Volumes/c/s/v/cp"])

    assert args.secret_scope == "rtmpack"
    assert args.input_topic == "rtmpack.04-routing.input"
    assert args.output_topic == "rtmpack.04-routing.out.rtm"
    assert args.checkpoint_interval == "5 minutes"
    assert args.kafka_linger_ms is None
    assert args.partitions is None


def test_kafka_linger_ms_partitions_and_checkpoint_args():
    base = ["--checkpoint-location", "/Volumes/c/s/v/cp"]
    assert routing_rtm.parse_args(base + ["--kafka-linger-ms", "0"]).kafka_linger_ms == 0
    assert routing_rtm.parse_args(base + ["--partitions", "8"]).partitions == 8
    assert routing_rtm.parse_args(base + ["--checkpoint-interval", "60"]).checkpoint_interval == "60 seconds"
    with pytest.raises(SystemExit):
        routing_rtm.parse_args(base + ["--kafka-linger-ms", "-1"])
    with pytest.raises(SystemExit):
        routing_rtm.parse_args([])


def test_all_only_mode_writes_one_row_per_event_to_all_topic(spark):
    import json as _json
    rows = [(b"e-%d" % i, _json.dumps({"event_id": f"e-{i}", "key": "user-1", "seq": i, "event_ts": 1, "is_dup": False,
                                         "type": t, "payload": "x"}).encode(), __import__("datetime").datetime(2026, 1, 1))
            for i, t in enumerate(["payment", "order", "click", "login", "other"])]
    raw = spark.createDataFrame(rows, "key binary, value binary, timestamp timestamp")
    out = routing_rtm.route_events(raw, "rtmpack.04-routing.out.rtm", mode="all-only").collect()
    assert len(out) == 5
    assert {r.topic for r in out} == {"rtmpack.04-routing.out.rtm.all"}


def test_all_fixed_mode_has_the_same_rows_as_all_only(spark):
    import json as _json
    rows = [(b"e-%d" % i, _json.dumps({"event_id": f"e-{i}", "key": "user-1", "seq": i, "event_ts": 1, "is_dup": False,
                                         "type": t, "payload": "x"}).encode(), __import__("datetime").datetime(2026, 1, 1))
            for i, t in enumerate(["payment", "order", "click"])]
    raw = spark.createDataFrame(rows, "key binary, value binary, timestamp timestamp")
    a = routing_rtm.route_events(raw, "b", mode="all-only").collect()
    b = routing_rtm.route_events(raw, "b", mode="all-fixed").collect()
    assert sorted(map(tuple, a)) == sorted(map(tuple, b))
    assert routing_rtm.parse_args(["--checkpoint-location", "/x", "--mode", "all-fixed"]).mode == "all-fixed"
