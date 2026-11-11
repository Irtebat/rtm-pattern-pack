"""Local tests for the 03-udf RTM transformation. Runs the pure function in batch mode."""

import csv
import datetime as dt
import json
import sys
from pathlib import Path

import pytest

sys.path.insert(0, str(Path(__file__).resolve().parents[1]))

from pyspark.sql import SparkSession  # noqa: E402
from pyspark.sql.types import BinaryType, StructField, StructType, TimestampType  # noqa: E402

import udf_rtm  # noqa: E402

# Shared with the Java test (flink/src/test/.../ScoreUdfTest.java).
VECTORS_FILE = Path(__file__).resolve().parents[2] / "score_vectors.csv"

KAFKA_SCHEMA = StructType(
    [
        StructField("key", BinaryType(), True),
        StructField("value", BinaryType(), True),
        StructField("timestamp", TimestampType(), True),
    ]
)

T0 = dt.datetime(2026, 1, 1, 0, 0, 0)


def load_vectors():
    with open(VECTORS_FILE, newline="", encoding="utf-8") as f:
        return [(r["event_id"], r["key"], int(r["score"]), r["decision"]) for r in csv.DictReader(f)]


VECTORS = load_vectors()


def _pyarrow_available():
    try:
        import pyarrow  # noqa: F401
        import pandas  # noqa: F401

        return True
    except ImportError:
        return False


needs_arrow = pytest.mark.skipif(not _pyarrow_available(), reason="pyarrow and pandas are not installed")


@pytest.fixture(scope="module")
def spark():
    session = (
        SparkSession.builder.master("local[2]")
        .appName("test-udf-rtm")
        .config("spark.sql.shuffle.partitions", "2")
        .config("spark.ui.enabled", "false")
        .getOrCreate()
    )
    udf_rtm.configure_arrow(session)
    yield session
    session.stop()


def _row(event_id, key, kafka_ts=T0, seq=7, event_ts=1767225600000, raw_value=None):
    value = raw_value
    if value is None:
        value = json.dumps(
            {
                "event_id": event_id,
                "key": key,
                "seq": seq,
                "event_ts": event_ts,
                "is_dup": False,
                "payload": "x" * 16,
            }
        )
    return (event_id.encode("utf-8"), value.encode("utf-8"), kafka_ts)


def _run(spark, rows, use_arrow):
    raw = spark.createDataFrame(rows, KAFKA_SCHEMA)
    return udf_rtm.score_events(raw, score_udf=udf_rtm.make_score_udf(use_arrow=use_arrow)).collect()


@pytest.fixture(params=[False, pytest.param(True, marks=needs_arrow)], ids=["python-udf", "arrow-udf"])
def use_arrow(request):
    return request.param


# --- the 03-udf score function ---


@pytest.mark.parametrize("event_id,key,score,decision", VECTORS)
def test_shared_vector_pure_function(event_id, key, score, decision):
    assert udf_rtm.score_decision(event_id, key) == (score, decision)


def test_vector_file_covers_all_decisions():
    assert len(VECTORS) == 5
    assert {v[3] for v in VECTORS} == {"flag", "review", "approve"}


def test_decision_thresholds():
    assert udf_rtm.decision_for(0) == "approve"
    assert udf_rtm.decision_for(799) == "approve"
    assert udf_rtm.decision_for(800) == "review"
    assert udf_rtm.decision_for(949) == "review"
    assert udf_rtm.decision_for(950) == "flag"
    assert udf_rtm.decision_for(999) == "flag"


def test_null_input_gives_null():
    assert udf_rtm.score_decision(None, "k") is None
    assert udf_rtm.score_decision("e", None) is None


# --- the transformation ---


def test_shared_vector_through_the_query(spark, use_arrow):
    out = _run(spark, [_row(e, k) for e, k, _, _ in VECTORS], use_arrow)
    by_key = {r["key"]: json.loads(r["value"]) for r in out}

    assert len(out) == len(VECTORS)
    for event_id, key, score, decision in VECTORS:
        body = by_key[event_id]
        assert body["key"] == key
        assert body["score"] == score
        assert body["decision"] == decision


def test_output_json_has_contract_fields_and_no_payload(spark, use_arrow):
    out = _run(spark, [_row("evt-0001", "user-00042", seq=11, event_ts=1767225600123)], use_arrow)

    assert [r["key"] for r in out] == ["evt-0001"]
    body = json.loads(out[0]["value"])
    assert set(body) == {"event_id", "key", "seq", "event_ts", "score", "decision", "in_ts", "engine"}
    assert body["event_id"] == "evt-0001"
    assert body["key"] == "user-00042"
    assert body["seq"] == 11
    assert body["event_ts"] == 1767225600123
    assert body["engine"] == "rtm"


def test_in_ts_is_kafka_timestamp_in_epoch_millis(spark, use_arrow):
    kafka_ts = dt.datetime(2026, 1, 1, 0, 0, 1, 250000, tzinfo=dt.timezone.utc)
    out = _run(spark, [_row("evt-0001", "user-00042", kafka_ts=kafka_ts)], use_arrow)

    assert json.loads(out[0]["value"])["in_ts"] == int(kafka_ts.timestamp() * 1000)


def test_null_and_malformed_inputs_produce_no_output(spark, use_arrow):
    rows = [
        _row("evt-bad", "k", raw_value="not json"),
        _row("evt-nokey", "k", raw_value=json.dumps({"event_id": "evt-nokey", "seq": 1})),
        _row("evt-ok", "user-00042"),
    ]

    out = _run(spark, rows, use_arrow)

    assert [r["key"] for r in out] == ["evt-ok"]


def test_one_record_per_arrow_batch_setting(spark):
    assert spark.conf.get("spark.sql.execution.arrow.maxRecordsPerBatch") == "1"


@needs_arrow
def test_udf_is_arrow_optimized():
    from pyspark.sql.pandas.functions import PythonEvalType

    assert udf_rtm.make_score_udf(use_arrow=True).evalType == PythonEvalType.SQL_ARROW_BATCHED_UDF
    assert udf_rtm.make_score_udf(use_arrow=False).evalType == PythonEvalType.SQL_BATCHED_UDF


# --- job plumbing ---


def test_kafka_sasl_options_use_shaded_scram_module():
    opts = udf_rtm.kafka_sasl_options("b:9096", "u", 'p"w')

    assert opts["kafka.security.protocol"] == "SASL_SSL"
    assert opts["kafka.sasl.mechanism"] == "SCRAM-SHA-512"
    assert opts["kafka.sasl.jaas.config"].startswith(udf_rtm.SCRAM_LOGIN_MODULE)
    assert 'password="p\\"w"' in opts["kafka.sasl.jaas.config"]


def test_parse_args_defaults():
    args = udf_rtm.parse_args(["--checkpoint-location", "/Volumes/c/s/v/cp"])

    assert args.secret_scope == "rtmpack"
    assert args.input_topic == "rtmpack.03-udf.input"
    assert args.output_topic == "rtmpack.03-udf.out.rtm"
    assert args.checkpoint_interval == "5 minutes"
    assert args.kafka_linger_ms is None
    assert args.partitions is None


def test_kafka_linger_ms_partitions_and_checkpoint_args():
    base = ["--checkpoint-location", "/Volumes/c/s/v/cp"]
    assert udf_rtm.parse_args(base + ["--kafka-linger-ms", "0"]).kafka_linger_ms == 0
    assert udf_rtm.parse_args(base + ["--partitions", "8"]).partitions == 8
    assert udf_rtm.parse_args(base + ["--checkpoint-interval", "60"]).checkpoint_interval == "60 seconds"
    with pytest.raises(SystemExit):
        udf_rtm.parse_args(base + ["--kafka-linger-ms", "-1"])
    with pytest.raises(SystemExit):
        udf_rtm.parse_args([])


def test_builtin_score_matches_shared_vectors(spark):
    """The built-in variant must give the same score and decision as the contract rule."""
    import csv
    from pathlib import Path

    rows = list(csv.DictReader(open(Path(__file__).resolve().parents[2] / "score_vectors.csv")))
    df = spark.createDataFrame([(r["event_id"], r["key"]) for r in rows], "event_id string, key string")
    got = df.select(udf_rtm.builtin_score(df.event_id, df.key).alias("s")).collect()
    for r, g in zip(rows, got):
        assert g.s.score == int(r["score"])
        assert g.s.decision == r["decision"]


def test_builtin_score_matches_python_rule_on_random_ids(spark):
    import uuid

    pairs = [(str(uuid.UUID(int=i * 7919 + 13)), f"user-{i:05d}") for i in range(500)]
    df = spark.createDataFrame(pairs, "event_id string, key string")
    got = df.select(udf_rtm.builtin_score(df.event_id, df.key).alias("s")).collect()
    for (eid, key), g in zip(pairs, got):
        assert (g.s.score, g.s.decision) == udf_rtm.score_decision(eid, key)


def test_parse_args_scoring_default_and_builtin():
    base = ["--checkpoint-location", "/tmp/x"]
    assert udf_rtm.parse_args(base).scoring == "arrow-udf"
    assert udf_rtm.parse_args(base + ["--scoring", "builtin"]).scoring == "builtin"
