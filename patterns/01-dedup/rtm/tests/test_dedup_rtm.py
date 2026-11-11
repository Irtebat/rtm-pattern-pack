"""Local tests for the 01-dedup RTM transformation. Runs the pure function in batch mode."""

import datetime as dt
import json
import sys
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

import dedup_rtm  # noqa: E402

KAFKA_SCHEMA = StructType(
    [
        StructField("key", BinaryType(), True),
        StructField("value", BinaryType(), True),
        StructField("timestamp", TimestampType(), True),
    ]
)


@pytest.fixture(scope="module")
def spark():
    session = (
        SparkSession.builder.master("local[2]")
        .appName("test-dedup-rtm")
        .config("spark.sql.shuffle.partitions", "2")
        .config("spark.ui.enabled", "false")
        .getOrCreate()
    )
    yield session
    session.stop()


def _kafka_row(event_id, key, seq, event_ts_ms, kafka_ts, is_dup=False, raw_value=None):
    value = raw_value
    if value is None:
        value = json.dumps(
            {
                "event_id": event_id,
                "key": key,
                "seq": seq,
                "event_ts": event_ts_ms,
                "is_dup": is_dup,
                "payload": "x" * 16,
            }
        )
    return (event_id.encode("utf-8") if event_id else b"", value.encode("utf-8"), kafka_ts)


def test_duplicates_collapse_to_one_row_per_event_id(spark):
    t0 = dt.datetime(2026, 1, 1, 0, 0, 0)
    rows = [
        _kafka_row("e-1", "user-1", 1, 1767225600000, t0),
        _kafka_row("e-1", "user-1", 1, 1767225600000, t0 + dt.timedelta(seconds=5), is_dup=True),
        _kafka_row("e-2", "user-2", 1, 1767225601000, t0 + dt.timedelta(seconds=1)),
        _kafka_row("e-2", "user-2", 1, 1767225601000, t0 + dt.timedelta(seconds=9), is_dup=True),
        _kafka_row("e-3", "user-3", 7, 1767225602000, t0 + dt.timedelta(seconds=2)),
    ]
    raw = spark.createDataFrame(rows, KAFKA_SCHEMA)

    out = dedup_rtm.dedup_first_per_event(raw, state_horizon="60 seconds").collect()

    keys = sorted(r["key"] for r in out)
    assert keys == ["e-1", "e-2", "e-3"]
    assert len(out) == len(set(keys))


def test_streaming_path_uses_watermarked_dedup(spark, tmp_path):
    """Exercise the watermarked streaming dedup path (dropDuplicates on event_id and event_time)."""
    t0 = dt.datetime(2026, 1, 1, 0, 0, 0)
    rows = [
        _kafka_row("s-1", "user-1", 1, 1767225600000, t0),
        _kafka_row("s-1", "user-1", 1, 1767225600000, t0 + dt.timedelta(seconds=3), is_dup=True),
        _kafka_row("s-2", "user-2", 1, 1767225600000, t0 + dt.timedelta(seconds=1)),
    ]
    src_dir = tmp_path / "src"
    spark.createDataFrame(rows, KAFKA_SCHEMA).coalesce(1).write.parquet(str(src_dir))

    stream = spark.readStream.schema(KAFKA_SCHEMA).parquet(str(src_dir))
    out = dedup_rtm.dedup_first_per_event(stream, state_horizon="60 seconds")
    query = (
        out.writeStream.format("memory")
        .queryName("dedup_stream_test")
        .outputMode("update")
        .option("checkpointLocation", str(tmp_path / "cp"))
        .trigger(availableNow=True)
        .start()
    )
    query.awaitTermination()

    keys = sorted(r["key"] for r in spark.sql("SELECT key FROM dedup_stream_test").collect())
    assert keys == ["s-1", "s-2"]


def test_output_json_has_contract_fields_and_engine(spark):
    t0 = dt.datetime(2026, 1, 1, 0, 0, 0)
    raw = spark.createDataFrame(
        [_kafka_row("e-9", "user-9", 42, 1767225600123, t0, is_dup=False)], KAFKA_SCHEMA
    )

    row = dedup_rtm.dedup_first_per_event(raw).collect()[0]
    body = json.loads(row["value"])

    assert set(body) == {"event_id", "key", "seq", "event_ts", "is_dup", "payload", "in_ts", "engine"}
    assert body["event_id"] == "e-9"
    assert body["key"] == "user-9"
    assert body["seq"] == 42
    assert body["event_ts"] == 1767225600123
    assert body["is_dup"] is False
    assert body["engine"] == "rtm"
    assert row["key"] == "e-9"


def test_in_ts_is_kafka_timestamp_in_epoch_millis(spark):
    kafka_ts = dt.datetime(2026, 1, 1, 0, 0, 0, 250000)  # .250 seconds
    raw = spark.createDataFrame(
        [_kafka_row("e-5", "user-5", 3, 1767225600000, kafka_ts)], KAFKA_SCHEMA
    )

    body = json.loads(dedup_rtm.dedup_first_per_event(raw).collect()[0]["value"])

    # Compare with the epoch-ms Spark reports for the same Kafka timestamp column.
    expected = raw.select(F.unix_millis("timestamp").alias("ms")).collect()[0]["ms"]
    assert body["in_ts"] == expected
    assert body["in_ts"] % 1000 == 250  # millisecond precision is preserved


def test_malformed_and_null_event_id_rows_are_dropped(spark):
    t0 = dt.datetime(2026, 1, 1, 0, 0, 0)
    rows = [
        _kafka_row("e-7", "user-7", 1, 1767225600000, t0),
        _kafka_row("", "user-x", 1, 1767225600000, t0, raw_value='{"key": "no-id"}'),
        _kafka_row("e-8", "user-8", 1, 1767225600000, t0, raw_value="not-json"),
    ]
    raw = spark.createDataFrame(rows, KAFKA_SCHEMA)

    keys = [r["key"] for r in dedup_rtm.dedup_first_per_event(raw).collect()]

    assert keys == ["e-7"]


def test_kafka_sasl_options_use_shaded_scram_module():
    opts = dedup_rtm.kafka_sasl_options("b-1:9096", 'us"er', "pa\\ss")

    assert opts["kafka.security.protocol"] == "SASL_SSL"
    assert opts["kafka.sasl.mechanism"] == "SCRAM-SHA-512"
    jaas = opts["kafka.sasl.jaas.config"]
    assert jaas.startswith(
        "kafkashaded.org.apache.kafka.common.security.scram.ScramLoginModule required"
    )
    assert 'username="us\\"er"' in jaas
    assert 'password="pa\\\\ss";' in jaas


def test_as_interval_accepts_seconds_or_interval_string():
    assert dedup_rtm.as_interval("60") == "60 seconds"
    assert dedup_rtm.as_interval("5 minutes") == "5 minutes"
    assert dedup_rtm.as_interval(" 60 seconds ") == "60 seconds"


def test_parse_args_defaults_match_contract():
    args = dedup_rtm.parse_args(["--checkpoint-location", "/Volumes/c/s/v/cp"])

    assert args.input_topic == "rtmpack.01-dedup.input"
    assert args.output_topic == "rtmpack.01-dedup.out.rtm"
    assert args.partitions == 8
    assert args.state_horizon == "60 seconds"
    assert args.checkpoint_interval == "60 seconds"
    assert args.secret_scope == "rtmpack"


def test_checkpoint_location_is_required():
    with pytest.raises(SystemExit):
        dedup_rtm.parse_args([])


def test_kafka_linger_ms_is_optional():
    base = ["--checkpoint-location", "/Volumes/c/s/v/cp"]
    assert dedup_rtm.parse_args(base).kafka_linger_ms is None
    assert dedup_rtm.parse_args(base + ["--kafka-linger-ms", "0"]).kafka_linger_ms == 0
    with pytest.raises(SystemExit):
        dedup_rtm.parse_args(base + ["--kafka-linger-ms", "-1"])
