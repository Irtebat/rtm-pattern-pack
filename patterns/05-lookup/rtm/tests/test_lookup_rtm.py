"""Local tests for the 05-lookup RTM transformation. Runs the pure function in batch mode."""

import datetime as dt
import hashlib
import json
import sys
from pathlib import Path

import pytest

sys.path.insert(0, str(Path(__file__).resolve().parents[1]))

from pyspark.sql import SparkSession  # noqa: E402
from pyspark.sql.types import BinaryType, StructField, StructType, TimestampType  # noqa: E402

import lookup_rtm  # noqa: E402

KAFKA_SCHEMA = StructType(
    [
        StructField("key", BinaryType(), True),
        StructField("value", BinaryType(), True),
        StructField("timestamp", TimestampType(), True),
    ]
)

T0 = dt.datetime(2026, 1, 1, 0, 0, 0)

RISK = ["low", "medium", "high"]
CATEGORY = ["grocery", "fuel", "travel", "electronics", "dining", "apparel", "health", "digital"]
COUNTRY = ["US", "GB", "DE", "FR", "IN", "JP", "BR", "CA", "AU", "SG"]


def merchant(i):
    """The 05-lookup rule (patterns/05-lookup/README.md, Benchmark setup) for merchant i."""
    h = int.from_bytes(hashlib.sha256(f"merchant|{i}".encode("utf-8")).digest()[:4], "big")
    return (
        f"m-{i:05d}",
        RISK[h % 3],
        CATEGORY[(h >> 8) % 8],
        COUNTRY[(h >> 16) % 10],
    )


@pytest.fixture(scope="module")
def spark():
    session = (
        SparkSession.builder.master("local[2]")
        .appName("test-05-lookup-lookup-rtm")
        .config("spark.sql.shuffle.partitions", "2")
        .config("spark.ui.enabled", "false")
        # Only the explicit broadcast() hint may trigger a broadcast join in the plan test.
        .config("spark.sql.autoBroadcastJoinThreshold", "-1")
        .getOrCreate()
    )
    yield session
    session.stop()


@pytest.fixture(scope="module")
def merchants_csv(tmp_path_factory):
    path = tmp_path_factory.mktemp("refdata") / "merchants.csv"
    lines = ["merchant_id,risk_tier,category,country"]
    lines += [",".join(merchant(i)) for i in range(50)]
    path.write_text("\n".join(lines) + "\n", encoding="utf-8")
    return str(path)


@pytest.fixture(scope="module")
def merchants(spark, merchants_csv):
    return lookup_rtm.read_merchants(spark, merchants_csv)


def _row(event_id, merchant_id, kafka_ts=T0, event_ts=1767225600000, raw_value=None):
    value = raw_value
    if value is None:
        value = json.dumps(
            {
                "event_id": event_id,
                "key": "user-00042",
                "seq": 7,
                "event_ts": event_ts,
                "is_dup": False,
                "merchant_id": merchant_id,
                "payload": "x" * 16,
            }
        )
    return (event_id.encode("utf-8"), value.encode("utf-8"), kafka_ts)


def _enrich(spark, merchants, rows):
    raw = spark.createDataFrame(rows, KAFKA_SCHEMA)
    return lookup_rtm.enrich_events(raw, merchants)


def test_reference_table_is_read_with_explicit_schema(merchants):
    assert [f.name for f in merchants.schema.fields] == [
        "merchant_id", "risk_tier", "category", "country",
    ]
    assert merchants.count() == 50
    assert merchants.filter("merchant_id = 'm-00003'").first().asDict() == dict(
        zip(["merchant_id", "risk_tier", "category", "country"], merchant(3))
    )


def test_output_json_has_contract_fields_and_values(spark, merchants):
    out = _enrich(spark, merchants, [_row("e-1", "m-00007", event_ts=1767225600123)]).collect()

    assert len(out) == 1
    assert out[0]["key"] == "e-1"
    body = json.loads(out[0]["value"])
    assert set(body) == {
        "event_id", "key", "event_ts", "merchant_id", "risk_tier", "category", "country",
        "in_ts", "engine",
    }
    _, tier, category, country = merchant(7)
    assert body["event_id"] == "e-1"
    assert body["key"] == "user-00042"
    assert body["event_ts"] == 1767225600123
    assert body["merchant_id"] == "m-00007"
    assert (body["risk_tier"], body["category"], body["country"]) == (tier, category, country)
    assert body["engine"] == "rtm"


def test_every_known_merchant_gets_its_own_attributes(spark, merchants):
    out = _enrich(spark, merchants, [_row(f"e-{i}", merchant(i)[0]) for i in range(50)]).collect()

    assert len(out) == 50
    for r in out:
        body = json.loads(r["value"])
        i = int(body["event_id"].split("-")[1])
        assert (body["merchant_id"], body["risk_tier"], body["category"], body["country"]) == merchant(i)


def test_unknown_null_and_malformed_inputs_produce_no_output(spark, merchants):
    rows = [
        _row("e-unknown", "m-99999"),
        _row("e-nullmerchant", None),
        _row("e-bad", "m-00001", raw_value="not json"),
        _row("e-ok", "m-00001"),
    ]

    out = _enrich(spark, merchants, rows).collect()

    assert [r["key"] for r in out] == ["e-ok"]


def test_in_ts_is_kafka_timestamp_in_epoch_millis(spark, merchants):
    kafka_ts = dt.datetime(2026, 1, 1, 0, 0, 1, 250000, tzinfo=dt.timezone.utc)
    out = _enrich(spark, merchants, [_row("e-1", "m-00001", kafka_ts=kafka_ts)]).collect()

    assert json.loads(out[0]["value"])["in_ts"] == int(kafka_ts.timestamp() * 1000)


def test_plan_uses_broadcast_join_without_shuffle_join(spark, merchants):
    df = _enrich(spark, merchants, [_row("e-1", "m-00001")])
    plan = df._jdf.queryExecution().executedPlan().toString()

    assert "BroadcastHashJoin" in plan, plan
    assert "SortMergeJoin" not in plan, plan


def test_kafka_sasl_options_use_shaded_scram_module():
    opts = lookup_rtm.kafka_sasl_options("b:9096", "u", 'p"w')

    assert opts["kafka.security.protocol"] == "SASL_SSL"
    assert opts["kafka.sasl.mechanism"] == "SCRAM-SHA-512"
    assert opts["kafka.sasl.jaas.config"].startswith(lookup_rtm.SCRAM_LOGIN_MODULE)
    assert 'password="p\\"w"' in opts["kafka.sasl.jaas.config"]


def test_parse_args_defaults():
    args = lookup_rtm.parse_args(
        ["--checkpoint-location", "/Volumes/c/s/v/cp", "--reference-path", "/Volumes/c/s/v/m.csv"]
    )

    assert args.secret_scope == "rtmpack"
    assert args.input_topic == "rtmpack.05-lookup.input"
    assert args.output_topic == "rtmpack.05-lookup.out.rtm"
    assert args.checkpoint_interval == "5 minutes"
    assert args.kafka_linger_ms is None
    assert args.reference_path == "/Volumes/c/s/v/m.csv"


def test_kafka_linger_ms_checkpoint_and_required_args():
    base = ["--checkpoint-location", "/Volumes/c/s/v/cp", "--reference-path", "/Volumes/c/s/v/m.csv"]
    assert lookup_rtm.parse_args(base + ["--kafka-linger-ms", "0"]).kafka_linger_ms == 0
    assert lookup_rtm.parse_args(base + ["--checkpoint-interval", "60"]).checkpoint_interval == "60 seconds"
    with pytest.raises(SystemExit):
        lookup_rtm.parse_args(base + ["--kafka-linger-ms", "-1"])
    with pytest.raises(SystemExit):
        lookup_rtm.parse_args(["--checkpoint-location", "/Volumes/c/s/v/cp"])
    with pytest.raises(SystemExit):
        lookup_rtm.parse_args(["--reference-path", "/Volumes/c/s/v/m.csv"])


@pytest.mark.parametrize("mode", ["read", "cached", "local"])
def test_reference_modes_give_same_table_and_broadcast_plan(spark, merchants, mode):
    ref = lookup_rtm.prepare_reference(spark, merchants, mode)
    assert sorted(ref.collect()) == sorted(merchants.collect())
    out = _enrich(spark, ref, [_row("e-1", "m-00003")])
    plan = out._jdf.queryExecution().executedPlan().toString()
    assert "BroadcastHashJoin" in plan
    assert out.count() == 1


def test_parse_args_reference_mode():
    base = ["--checkpoint-location", "/tmp/x", "--reference-path", "/tmp/m.csv"]
    assert lookup_rtm.parse_args(base).reference_mode == "read"
    assert lookup_rtm.parse_args(base + ["--reference-mode", "local"]).reference_mode == "local"
