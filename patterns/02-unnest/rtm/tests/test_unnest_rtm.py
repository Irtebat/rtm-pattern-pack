"""Local tests for the 02-unnest RTM transformation. Runs the pure function in batch mode."""

import datetime as dt
import json
import sys
from pathlib import Path

import pytest

sys.path.insert(0, str(Path(__file__).resolve().parents[1]))

from pyspark.sql import SparkSession  # noqa: E402
from pyspark.sql.types import BinaryType, StructField, StructType, TimestampType  # noqa: E402

import unnest_rtm  # noqa: E402

KAFKA_SCHEMA = StructType(
    [
        StructField("key", BinaryType(), True),
        StructField("value", BinaryType(), True),
        StructField("timestamp", TimestampType(), True),
    ]
)

T0 = dt.datetime(2026, 1, 1, 0, 0, 0)


@pytest.fixture(scope="module")
def spark():
    session = (
        SparkSession.builder.master("local[2]")
        .appName("test-unnest-rtm")
        .config("spark.sql.shuffle.partitions", "2")
        .config("spark.ui.enabled", "false")
        .getOrCreate()
    )
    yield session
    session.stop()


def _items(n):
    return [{"sku": f"sku-{i}", "qty": i + 1, "price": 1.5 * (i + 1)} for i in range(n)]


def _row(order_id, items, kafka_ts=T0, event_ts=1767225600000, raw_value=None):
    value = raw_value
    if value is None:
        value = json.dumps(
            {
                "order_id": order_id,
                "key": "cust-00042",
                "event_ts": event_ts,
                "items": items,
                "payload": "x" * 16,
            }
        )
    return (order_id.encode("utf-8"), value.encode("utf-8"), kafka_ts)


def _run(spark, rows):
    raw = spark.createDataFrame(rows, KAFKA_SCHEMA)
    return unnest_rtm.unnest_items(raw).collect()


def test_one_record_per_item_with_zero_based_index_keys(spark):
    rows = [_row("o-1", _items(3)), _row("o-2", _items(1))]

    out = _run(spark, rows)

    assert sorted(r["key"] for r in out) == ["o-1:0", "o-1:1", "o-1:2", "o-2:0"]


def test_output_json_has_contract_fields(spark):
    out = _run(spark, [_row("o-9", _items(2), event_ts=1767225600123)])
    by_key = {r["key"]: json.loads(r["value"]) for r in out}

    body = by_key["o-9:1"]
    assert set(body) == {
        "order_id", "key", "event_ts", "item_idx", "sku", "qty", "price", "in_ts", "engine",
    }
    assert body["order_id"] == "o-9"
    assert body["key"] == "cust-00042"
    assert body["event_ts"] == 1767225600123
    assert body["item_idx"] == 1
    assert body["sku"] == "sku-1"
    assert body["qty"] == 2
    assert body["price"] == 3.0
    assert body["engine"] == "rtm"


def test_in_ts_is_kafka_timestamp_in_epoch_millis_for_every_item(spark):
    kafka_ts = dt.datetime(2026, 1, 1, 0, 0, 1, 250000, tzinfo=dt.timezone.utc)
    out = _run(spark, [_row("o-1", _items(3), kafka_ts=kafka_ts)])

    expected = int(kafka_ts.timestamp() * 1000)
    assert [json.loads(r["value"])["in_ts"] for r in out] == [expected] * 3


def test_total_output_equals_sum_of_item_counts(spark):
    sizes = [1, 2, 3, 4, 5]
    out = _run(spark, [_row(f"o-{i}", _items(n)) for i, n in enumerate(sizes)])

    assert len(out) == sum(sizes)
    assert len({r["key"] for r in out}) == sum(sizes)


def test_empty_null_and_malformed_inputs_produce_no_output(spark):
    rows = [
        _row("o-empty", []),
        _row("o-null", None),
        _row("o-bad", [], raw_value="not json"),
        _row("o-ok", _items(1)),
    ]

    out = _run(spark, rows)

    assert [r["key"] for r in out] == ["o-ok:0"]


def test_kafka_sasl_options_use_shaded_scram_module():
    opts = unnest_rtm.kafka_sasl_options("b:9096", "u", 'p"w')

    assert opts["kafka.security.protocol"] == "SASL_SSL"
    assert opts["kafka.sasl.mechanism"] == "SCRAM-SHA-512"
    assert opts["kafka.sasl.jaas.config"].startswith(unnest_rtm.SCRAM_LOGIN_MODULE)
    assert 'password="p\\"w"' in opts["kafka.sasl.jaas.config"]


def test_parse_args_defaults():
    args = unnest_rtm.parse_args(["--checkpoint-location", "/Volumes/c/s/v/cp"])

    assert args.secret_scope == "rtmpack"
    assert args.input_topic == "rtmpack.02-unnest.input"
    assert args.output_topic == "rtmpack.02-unnest.out.rtm"
    assert args.checkpoint_interval == "5 minutes"
    assert args.kafka_linger_ms is None


def test_kafka_linger_ms_and_checkpoint_args():
    base = ["--checkpoint-location", "/Volumes/c/s/v/cp"]
    assert unnest_rtm.parse_args(base + ["--kafka-linger-ms", "0"]).kafka_linger_ms == 0
    assert unnest_rtm.parse_args(base + ["--checkpoint-interval", "60"]).checkpoint_interval == "60 seconds"
    with pytest.raises(SystemExit):
        unnest_rtm.parse_args(base + ["--kafka-linger-ms", "-1"])
    with pytest.raises(SystemExit):
        unnest_rtm.parse_args([])


def _rows_mixed():
    return [
        _row("o-1", _items(3)),
        _row("o-2", _items(1), event_ts=1767225600999),
        _row("o-3", []),
        _row("o-4", None),
        _row("o-bad", None, raw_value="not json"),
        _row("o-5", _items(5)),
    ]


def test_variant_parse_gives_the_same_output_as_from_json(spark):
    raw = spark.createDataFrame(_rows_mixed(), KAFKA_SCHEMA)
    a = sorted((r["key"], r["value"]) for r in unnest_rtm.unnest_items(raw, parse="from-json").collect())
    b = sorted((r["key"], r["value"]) for r in unnest_rtm.unnest_items(raw, parse="variant").collect())
    assert len(a) == 9
    assert a == b


def test_variant_parse_arg():
    base = ["--checkpoint-location", "/tmp/cp"]
    assert unnest_rtm.parse_args(base).parse == "from-json"
    assert unnest_rtm.parse_args(base + ["--parse", "variant"]).parse == "variant"
    with pytest.raises(SystemExit):
        unnest_rtm.parse_args(base + ["--parse", "other"])
