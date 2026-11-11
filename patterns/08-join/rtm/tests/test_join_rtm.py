"""Local tests for the 08-join RTM transformation. Runs the pure function in batch mode and in a
finite streaming query."""

import datetime as dt
import json
import sys
from pathlib import Path

import pytest

sys.path.insert(0, str(Path(__file__).resolve().parents[1]))

from pyspark.sql import SparkSession  # noqa: E402
from pyspark.sql import functions as F  # noqa: E402
from pyspark.sql.types import BinaryType, StructField, StructType, TimestampType  # noqa: E402

import join_rtm  # noqa: E402

KAFKA_SCHEMA = StructType(
    [
        StructField("key", BinaryType(), True),
        StructField("value", BinaryType(), True),
        StructField("timestamp", TimestampType(), True),
    ]
)

T0_MS = 1_800_000_000_000
T0 = dt.datetime(2026, 1, 1, 0, 0, 0)


@pytest.fixture(scope="module")
def spark():
    session = (
        SparkSession.builder.master("local[2]")
        .appName("test-join-rtm")
        .config("spark.sql.shuffle.partitions", "2")
        .config("spark.ui.enabled", "false")
        .getOrCreate()
    )
    yield session
    session.stop()


def _order(order_id, event_ts=T0_MS, kafka_ts=T0, key="cust-7", amount=1234, raw_value=None):
    value = raw_value or json.dumps(
        {"order_id": order_id, "key": key, "event_ts": event_ts, "amount": amount, "payload": "x" * 8}
    )
    return (key.encode(), value.encode(), kafka_ts)


def _payment(payment_id, order_id, event_ts, kafka_ts=T0, method="card", raw_value=None):
    value = raw_value or json.dumps(
        {"payment_id": payment_id, "order_id": order_id, "event_ts": event_ts, "method": method, "payload": "y" * 8}
    )
    return (b"cust-7", value.encode(), kafka_ts)


def _df(spark, rows):
    return spark.createDataFrame(rows, KAFKA_SCHEMA)


def _run(spark, orders, payments, **kwargs):
    out = join_rtm.join_orders_payments(_df(spark, orders), _df(spark, payments), **kwargs).collect()
    return {r["key"]: json.loads(r["value"]) for r in out}, out


def test_join_condition_boundaries(spark):
    orders = [_order(i) for i in ("at0", "at30", "at31", "before")]
    payments = [
        _payment("p-at0", "at0", T0_MS),
        _payment("p-at30", "at30", T0_MS + 30_000),
        _payment("p-at31", "at31", T0_MS + 31_000),
        _payment("p-before", "before", T0_MS - 1),
    ]

    by_order, rows = _run(spark, orders, payments)

    assert len(rows) == 2
    assert sorted(by_order) == ["at0", "at30"]
    assert by_order["at0"]["payment_id"] == "p-at0"
    assert by_order["at30"]["payment_id"] == "p-at30"


def test_just_outside_the_window_by_one_millisecond(spark):
    by_order, _ = _run(
        spark,
        [_order("a"), _order("b")],
        [_payment("pa", "a", T0_MS + 30_001), _payment("pb", "b", T0_MS + 30_000)],
    )
    assert sorted(by_order) == ["b"]


def test_unmatched_orders_and_payments_produce_no_output(spark):
    by_order, rows = _run(
        spark,
        [_order("unpaid"), _order("paid")],
        [_payment("orphan", "nobody", T0_MS), _payment("p1", "paid", T0_MS + 100)],
    )
    assert len(rows) == 1
    assert sorted(by_order) == ["paid"]


def test_output_fields_key_and_in_ts(spark):
    kafka_o = dt.datetime(2026, 1, 1, 0, 0, 0, 250000)
    kafka_p = dt.datetime(2026, 1, 1, 0, 0, 1, 750000)
    orders = _df(spark, [_order("o-9", event_ts=T0_MS + 5, kafka_ts=kafka_o, key="cust-9", amount=777)])
    payments = _df(spark, [_payment("p-9", "o-9", T0_MS + 1005, kafka_ts=kafka_p, method="wallet")])

    row = join_rtm.join_orders_payments(orders, payments).collect()[0]
    body = json.loads(row["value"])

    assert row["key"] == "o-9"
    assert set(body) == {
        "order_id", "payment_id", "key", "amount", "method", "order_ts", "payment_ts", "in_ts", "engine",
    }
    assert body["order_id"] == "o-9"
    assert body["payment_id"] == "p-9"
    assert body["key"] == "cust-9"
    assert body["amount"] == 777
    assert body["method"] == "wallet"
    assert body["order_ts"] == T0_MS + 5
    assert body["payment_ts"] == T0_MS + 1005
    assert body["engine"] == "rtm"
    expected = _df(spark, [_order("x", kafka_ts=kafka_p)]).select(F.unix_millis("timestamp")).collect()[0][0]
    assert body["in_ts"] == expected  # the later Kafka timestamp (payment)
    assert body["in_ts"] % 1000 == 750


def test_in_ts_is_the_larger_input_in_ts_in_both_directions(spark):
    early = dt.datetime(2026, 1, 1, 0, 0, 0, 1000)
    late = dt.datetime(2026, 1, 1, 0, 0, 0, 999000)
    by_order, _ = _run(
        spark,
        [_order("a", kafka_ts=late), _order("b", kafka_ts=early)],
        [_payment("pa", "a", T0_MS + 10, kafka_ts=early), _payment("pb", "b", T0_MS + 10, kafka_ts=late)],
    )
    assert by_order["a"]["in_ts"] == by_order["b"]["in_ts"]
    assert by_order["a"]["in_ts"] % 1000 == 999


def test_malformed_rows_are_dropped(spark):
    orders = [_order("ok"), _order("bad", raw_value="not-json"), _order("noid", raw_value='{"key": "k"}')]
    payments = [_payment("p", "ok", T0_MS + 1), _payment("q", "bad", T0_MS + 1, raw_value="{")]
    by_order, _ = _run(spark, orders, payments)
    assert sorted(by_order) == ["ok"]


def test_custom_join_window(spark):
    by_order, _ = _run(
        spark,
        [_order("a")],
        [_payment("pa", "a", T0_MS + 5_000)],
        join_window="2 seconds",
    )
    assert by_order == {}


def test_streaming_join_with_watermarks(spark, tmp_path):
    """Run the join as a finite streaming query (file sources, memory sink)."""
    orders = [_order("s-1", event_ts=T0_MS), _order("s-2", event_ts=T0_MS + 1000), _order("s-3", event_ts=T0_MS)]
    payments = [
        _payment("p-1", "s-1", T0_MS + 2000),  # inside
        _payment("p-2", "s-2", T0_MS + 1000 + 31_000),  # outside
    ]
    o_dir, p_dir = tmp_path / "orders", tmp_path / "payments"
    _df(spark, orders).coalesce(1).write.parquet(str(o_dir))
    _df(spark, payments).coalesce(1).write.parquet(str(p_dir))

    o_stream = spark.readStream.schema(KAFKA_SCHEMA).parquet(str(o_dir))
    p_stream = spark.readStream.schema(KAFKA_SCHEMA).parquet(str(p_dir))
    out = join_rtm.join_orders_payments(o_stream, p_stream)
    assert out.isStreaming

    query = (
        out.writeStream.format("memory")
        .queryName("join_stream_test")
        .outputMode("append")
        .option("checkpointLocation", str(tmp_path / "cp"))
        .trigger(availableNow=True)
        .start()
    )
    query.awaitTermination()

    rows = spark.sql("SELECT key, value FROM join_stream_test").collect()
    assert [r["key"] for r in rows] == ["s-1"]
    assert json.loads(rows[0]["value"])["payment_id"] == "p-1"


def test_stream_join_confs_are_the_five_from_the_setup_page():
    assert join_rtm.STREAM_JOIN_CONFS == {
        "spark.databricks.streaming.realTimeMode.streamStreamJoin.enabled": "true",
        "spark.sql.streaming.join.stateFormatVersion": "4",
        "spark.sql.streaming.join.stateFormatV4.enabled": "true",
        "spark.sql.streaming.stateStore.rocksdb.mergeOperatorVersion": "2",
        "spark.sql.streaming.realTimeMode.controlMessage.enabled": "true",
    }


def test_apply_join_confs_sets_each_value_and_reports_rejections(capsys):
    class FakeConf:
        def __init__(self):
            self.values = {}

        def set(self, name, value):
            if name.endswith("mergeOperatorVersion"):
                raise RuntimeError("cannot modify at runtime")
            self.values[name] = value

    class FakeSpark:
        conf = FakeConf()

    join_rtm.apply_join_confs(FakeSpark())

    assert len(FakeSpark.conf.values) == 4
    assert "mergeOperatorVersion" in capsys.readouterr().err


def test_kafka_sasl_options_use_shaded_scram_module():
    opts = join_rtm.kafka_sasl_options("b-1:9096", 'us"er', "pa\\ss")
    assert opts["kafka.security.protocol"] == "SASL_SSL"
    assert opts["kafka.sasl.mechanism"] == "SCRAM-SHA-512"
    jaas = opts["kafka.sasl.jaas.config"]
    assert jaas.startswith("kafkashaded.org.apache.kafka.common.security.scram.ScramLoginModule required")
    assert 'username="us\\"er"' in jaas
    assert 'password="pa\\\\ss";' in jaas


def test_as_interval_accepts_seconds_or_interval_string():
    assert join_rtm.as_interval("30") == "30 seconds"
    assert join_rtm.as_interval("5 minutes") == "5 minutes"


def test_parse_args_defaults_match_contract():
    args = join_rtm.parse_args(["--checkpoint-location", "/Volumes/c/s/v/cp"])
    assert args.input_topic == "rtmpack.08-join.input"
    assert args.output_topic == "rtmpack.08-join.out.rtm"
    assert args.partitions == 4
    assert args.watermark_delay == "30 seconds"
    assert args.join_window == "30 seconds"
    assert args.checkpoint_interval == "60 seconds"
    assert args.secret_scope == "rtmpack"


def test_checkpoint_location_is_required():
    with pytest.raises(SystemExit):
        join_rtm.parse_args([])


def test_kafka_linger_ms_is_optional():
    base = ["--checkpoint-location", "/Volumes/c/s/v/cp"]
    assert join_rtm.parse_args(base).kafka_linger_ms is None
    assert join_rtm.parse_args(base + ["--kafka-linger-ms", "0"]).kafka_linger_ms == 0
    with pytest.raises(SystemExit):
        join_rtm.parse_args(base + ["--kafka-linger-ms", "-1"])
