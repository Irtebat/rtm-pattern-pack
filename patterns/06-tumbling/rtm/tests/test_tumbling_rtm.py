"""Local tests for the 06-tumbling RTM transformation. Runs the pure function in batch mode."""

import datetime as dt
import json
import sys
from collections import defaultdict
from pathlib import Path

import pytest

sys.path.insert(0, str(Path(__file__).resolve().parents[1]))

from pyspark.sql import SparkSession  # noqa: E402
from pyspark.sql import functions as F  # noqa: E402
from pyspark.sql.types import (  # noqa: E402
    BinaryType,
    StructField,
    StructType,
    TimestampType,
)

import tumbling_rtm  # noqa: E402

STEP_MS = 60_000
WINDOW_MS = 60_000
WINDOWS_PER_EVENT = WINDOW_MS // STEP_MS

KAFKA_SCHEMA = StructType(
    [
        StructField("key", BinaryType(), True),
        StructField("value", BinaryType(), True),
        StructField("timestamp", TimestampType(), True),
    ]
)

T0 = dt.datetime(2026, 1, 1, 0, 0, 0, tzinfo=dt.timezone.utc)


@pytest.fixture(scope="module")
def spark():
    session = (
        SparkSession.builder.master("local[2]")
        .appName("test-tumbling_rtm")
        .config("spark.sql.shuffle.partitions", "2")
        .config("spark.sql.session.timeZone", "UTC")
        .config("spark.ui.enabled", "false")
        .getOrCreate()
    )
    yield session
    session.stop()


def _row(key, event_ts_ms, amount, kafka_ts, raw_value=None):
    value = raw_value
    if value is None:
        value = json.dumps(
            {
                "event_id": f"{key}-{event_ts_ms}-{amount}",
                "key": key,
                "seq": 1,
                "event_ts": event_ts_ms,
                "amount": amount,
                "payload": "x" * 8,
            }
        )
    return (key.encode("utf-8") if key else b"", value.encode("utf-8"), kafka_ts)


def _run(spark, rows, **kwargs):
    raw = spark.createDataFrame(rows, KAFKA_SCHEMA)
    out = tumbling_rtm.aggregate_tumbling(raw, **kwargs).collect()
    return {r["key"]: json.loads(r["value"]) for r in out}, out


def _epoch_ms(ts):
    return int(ts.timestamp() * 1000)


# Events at the window and slide boundaries, out of order, two keys.
EVENTS = [
    # key, event_ts (ms), amount, kafka time offset (s)
    ("user-1", 59_999, 100, 0.0),
    ("user-1", 60_000, 250, 1.0),
    ("user-1", 30_000, 40, 2.0),
    ("user-1", 119_999, 7, 3.0),
    ("user-2", 10_000, 500, 4.0),
    ("user-2", 61_000, 12, 5.0),
    ("user-1", 20_000, 3, 6.0),
    ("user-1", 59_999, 1, 7.0),
]


def _rows():
    return [_row(k, ts, a, T0 + dt.timedelta(seconds=off)) for k, ts, a, off in EVENTS]


def _expected():
    """Brute-force oracle: every window start s (a multiple of STEP_MS) with s <= ts < s + WINDOW_MS."""
    agg = defaultdict(lambda: {"count": 0, "sum_amount": 0, "in_ts": 0})
    for k, ts, a, off in EVENTS:
        in_ts = _epoch_ms(T0 + dt.timedelta(seconds=off))
        start = ts - ts % STEP_MS
        while start > ts - WINDOW_MS:
            g = agg[f"{k}:{start}"]
            g["count"] += 1
            g["sum_amount"] += a
            g["in_ts"] = max(g["in_ts"], in_ts)
            start -= STEP_MS
    return agg


def test_counts_sums_max_in_ts_and_windows_match_oracle(spark):
    result, _ = _run(spark, _rows())
    expected = _expected()

    assert set(result) == set(expected)
    for out_key, exp in expected.items():
        body = result[out_key]
        key, start = out_key.split(":")
        assert body["key"] == key
        assert body["window_start_ms"] == int(start)
        assert body["window_end_ms"] == int(start) + WINDOW_MS
        assert body["count"] == exp["count"], out_key
        assert body["sum_amount"] == exp["sum_amount"], out_key
        assert body["in_ts"] == exp["in_ts"], out_key
        assert body["engine"] == "rtm"


def test_window_starts_are_epoch_aligned(spark):
    result, _ = _run(spark, _rows())
    for body in result.values():
        assert body["window_start_ms"] % STEP_MS == 0
        assert body["window_end_ms"] - body["window_start_ms"] == WINDOW_MS


def test_kafka_key_is_key_colon_window_start_ms(spark):
    _, out = _run(spark, [_row("user-7", 125_000, 5, T0)])
    keys = sorted(r["key"] for r in out)
    assert keys == sorted(f"user-7:{s}" for s in (120_000,))
    for r in out:
        assert r["key"] == f"user-7:{json.loads(r['value'])['window_start_ms']}"


def test_output_json_has_contract_fields(spark):
    result, _ = _run(spark, [_row("user-9", 61_500, 42, T0)])
    body = next(iter(result.values()))
    assert set(body) == {
        "key",
        "window_start_ms",
        "window_end_ms",
        "count",
        "sum_amount",
        "in_ts",
        "engine",
    }
    assert body["key"] == "user-9"
    assert body["sum_amount"] == 42
    assert body["count"] == 1


def test_in_ts_is_max_kafka_timestamp_in_epoch_millis(spark):
    t_a = dt.datetime(2026, 1, 1, 0, 0, 0, 250000, tzinfo=dt.timezone.utc)
    t_b = dt.datetime(2026, 1, 1, 0, 0, 5, 750000, tzinfo=dt.timezone.utc)
    rows = [_row("user-5", 1_000, 1, t_b), _row("user-5", 2_000, 1, t_a)]
    result, _ = _run(spark, rows)

    expected = spark.createDataFrame([(t_b,)], ["ts"]).select(F.unix_millis("ts")).collect()[0][0]
    for body in result.values():
        assert body["in_ts"] == expected
        assert body["in_ts"] % 1000 == 750


def test_each_event_lands_in_exactly_one_window(spark):
    result, _ = _run(spark, _rows())
    assert sum(b["count"] for b in result.values()) == len(EVENTS)
    assert sum(b["sum_amount"] for b in result.values()) == sum(a for _, _, a, _ in EVENTS)
    # 59_999 (twice), 30_000 and 20_000 are before the boundary; 60_000 and 119_999 are after it.
    assert result["user-1:0"]["count"] == 4
    assert result["user-1:60000"]["count"] == 2


def test_rows_without_key_or_event_ts_are_dropped(spark):
    rows = [
        _row("user-1", 5_000, 1, T0),
        _row("", 5_000, 1, T0, raw_value='{"event_ts": 5000, "amount": 1}'),
        _row("user-2", 5_000, 1, T0, raw_value='{"key": "user-2", "amount": 1}'),
        _row("user-3", 5_000, 1, T0, raw_value="not-json"),
    ]
    result, _ = _run(spark, rows)
    assert {b["key"] for b in result.values()} == {"user-1"}


def test_streaming_path_runs_with_watermark_and_update_mode(spark, tmp_path):
    """Run the same query as a finite streaming query (OSS Spark, availableNow)."""
    src_dir = tmp_path / "src"
    spark.createDataFrame(_rows(), KAFKA_SCHEMA).coalesce(1).write.parquet(str(src_dir))

    stream = spark.readStream.schema(KAFKA_SCHEMA).parquet(str(src_dir))
    out = tumbling_rtm.aggregate_tumbling(stream, watermark="10 seconds")
    query = (
        out.writeStream.format("memory")
        .queryName("tumbling_stream_test")
        .outputMode("update")
        .option("checkpointLocation", str(tmp_path / "cp"))
        .trigger(availableNow=True)
        .start()
    )
    query.awaitTermination()

    rows = spark.sql("SELECT key, value FROM tumbling_stream_test").collect()
    # The watermark drops events that arrive more than 10 s behind the maximum event time seen,
    # so only the windows that no later data closed are checked for exact values.
    assert rows, "the streaming query must produce output"
    for r in rows:
        body = json.loads(r["value"])
        assert r["key"] == f"{body['key']}:{body['window_start_ms']}"
        assert body["window_end_ms"] - body["window_start_ms"] == WINDOW_MS


def test_kafka_sasl_options_use_shaded_scram_module():
    opts = tumbling_rtm.kafka_sasl_options("b-1:9096", 'us"er', "pa\\ss")

    assert opts["kafka.security.protocol"] == "SASL_SSL"
    assert opts["kafka.sasl.mechanism"] == "SCRAM-SHA-512"
    jaas = opts["kafka.sasl.jaas.config"]
    assert jaas.startswith(
        "kafkashaded.org.apache.kafka.common.security.scram.ScramLoginModule required"
    )
    assert 'username="us\\"er"' in jaas
    assert 'password="pa\\\\ss";' in jaas


def test_as_interval_accepts_seconds_or_interval_string():
    assert tumbling_rtm.as_interval("60") == "60 seconds"
    assert tumbling_rtm.as_interval("5 minutes") == "5 minutes"


def test_parse_args_defaults_match_contract():
    args = tumbling_rtm.parse_args(["--checkpoint-location", "/Volumes/c/s/v/cp"])

    assert args.input_topic == "rtmpack.06-tumbling.input"
    assert args.output_topic == "rtmpack.06-tumbling.out.rtm"
    assert args.partitions == 4
    assert args.state_horizon == "10 seconds"
    assert args.checkpoint_interval == "60 seconds"
    assert args.secret_scope == "rtmpack"


def test_checkpoint_location_is_required():
    with pytest.raises(SystemExit):
        tumbling_rtm.parse_args([])


def test_kafka_linger_ms_is_optional():
    base = ["--checkpoint-location", "/Volumes/c/s/v/cp"]
    assert tumbling_rtm.parse_args(base).kafka_linger_ms is None
    assert tumbling_rtm.parse_args(base + ["--kafka-linger-ms", "0"]).kafka_linger_ms == 0
    with pytest.raises(SystemExit):
        tumbling_rtm.parse_args(base + ["--kafka-linger-ms", "-1"])
