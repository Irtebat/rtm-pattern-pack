"""Pattern 03-udf: scalar UDF scoring on Spark Real-Time Mode.

Reads the contract input topic from Amazon MSK (SASL_SSL, SCRAM-SHA-512), scores every
event with an Arrow-optimized Python scalar UDF and writes the contract output topic with
key = event_id and a JSON value. The score rule is in patterns/03-udf/README.md, Benchmark setup:

    h        = SHA-256(UTF-8(event_id + "|" + key))
    score    = (first 4 bytes of h as an unsigned big-endian integer) mod 1000
    decision = "flag" if score >= 950, "review" if score >= 800, else "approve"

The UDF is declared with @udf(..., useArrow=True), the Arrow-optimized Python UDF of
PySpark 4.1 (spark.sql.execution.pythonUDF.arrow.enabled has the same effect globally).
The job sets spark.sql.execution.arrow.maxRecordsPerBatch=1 so a batch holds one record
and a record is not held back waiting for a full Arrow batch.

Runs as a Databricks Python task on a dedicated classic cluster with
spark.databricks.streaming.realTimeMode.enabled=true. The transformation (score_events) is a
pure function so it can be unit tested in batch mode. The query has no shuffle, so tasks =
Kafka input partitions.
"""

import argparse
import hashlib
import sys
from typing import Optional, Sequence, Tuple

from pyspark.sql import DataFrame, SparkSession
from pyspark.sql import functions as F
from pyspark.sql.types import BooleanType, IntegerType, LongType, StringType, StructField, StructType

ENGINE = "rtm"
SECRET_SCOPE = "rtmpack"
SCRAM_LOGIN_MODULE = "kafkashaded.org.apache.kafka.common.security.scram.ScramLoginModule"

# patterns/03-udf/README.md, Benchmark setup: RTM sets one record per Arrow batch.
ARROW_MAX_RECORDS_PER_BATCH = 1
FLAG_THRESHOLD = 950
REVIEW_THRESHOLD = 800

# Contract section 2: input event JSON value.
INPUT_EVENT_SCHEMA = StructType(
    [
        StructField("event_id", StringType(), True),
        StructField("key", StringType(), True),
        StructField("seq", LongType(), True),
        StructField("event_ts", LongType(), True),  # epoch milliseconds
        StructField("is_dup", BooleanType(), True),
        StructField("payload", StringType(), True),
    ]
)

SCORE_RESULT_SCHEMA = StructType(
    [
        StructField("score", IntegerType(), True),
        StructField("decision", StringType(), True),
    ]
)


def decision_for(score: int) -> str:
    if score >= FLAG_THRESHOLD:
        return "flag"
    if score >= REVIEW_THRESHOLD:
        return "review"
    return "approve"


def score_decision(event_id: Optional[str], key: Optional[str]) -> Optional[Tuple[int, str]]:
    """The 03-udf score function. Returns (score, decision), or None if an input is null."""
    if event_id is None or key is None:
        return None
    digest = hashlib.sha256(f"{event_id}|{key}".encode("utf-8")).digest()
    score = int.from_bytes(digest[:4], "big") % 1000
    return score, decision_for(score)


def make_score_udf(use_arrow: bool = True):
    """Builds the scoring UDF. Production always uses Arrow; tests may turn it off."""
    from pyspark.sql.functions import udf

    return udf(score_decision, returnType=SCORE_RESULT_SCHEMA, useArrow=use_arrow)


def builtin_score(event_id, key):
    """The 03-udf score and decision with built-in functions only (no Python UDF).

    sha2 hashes the UTF-8 bytes of the string; the first 8 hex digits are the first
    4 bytes, read as an unsigned big-endian integer by conv(..., 16, 10). Used by the
    variant run that removes the Python round trip (see the pattern README).
    """
    h = F.sha2(F.concat(event_id, F.lit("|"), key), 256)
    score = F.pmod(F.conv(F.substring(h, 1, 8), 16, 10).cast("long"), F.lit(1000)).cast("int")
    decision = (
        F.when(score >= 950, F.lit("flag")).when(score >= 800, F.lit("review")).otherwise(F.lit("approve"))
    )
    return F.when(event_id.isNull() | key.isNull(), F.lit(None)).otherwise(
        F.struct(score.alias("score"), decision.alias("decision"))
    )


def score_events(raw: DataFrame, engine: str = ENGINE, score_udf=None) -> DataFrame:
    """Score every event.

    `raw` has the Kafka source schema (key, value, topic, partition, offset,
    timestamp, timestampType). Returns a DataFrame with two columns:
      key   - event_id
      value - the output JSON: event_id, key, seq, event_ts, score, decision, in_ts, engine
    Rows with a null event_id or key (including malformed JSON) produce no output.
    """
    if score_udf is None:
        score_udf = make_score_udf()

    parsed = raw.select(
        F.from_json(F.col("value").cast("string"), INPUT_EVENT_SCHEMA).alias("e"),
        F.unix_millis(F.col("timestamp")).alias("in_ts"),
    ).filter(F.col("e.event_id").isNotNull() & F.col("e.key").isNotNull())

    scored = parsed.select(
        F.col("e.event_id").alias("event_id"),
        F.col("e.key").alias("key"),
        F.col("e.seq").alias("seq"),
        F.col("e.event_ts").alias("event_ts"),
        F.col("in_ts"),
        score_udf(F.col("e.event_id"), F.col("e.key")).alias("s"),
    )

    return scored.select(
        F.col("event_id").alias("key"),
        F.to_json(
            F.struct(
                F.col("event_id"),
                F.col("key"),
                F.col("seq"),
                F.col("event_ts"),
                F.col("s.score").alias("score"),
                F.col("s.decision").alias("decision"),
                F.col("in_ts"),
                F.lit(engine).alias("engine"),
            )
        ).alias("value"),
    )


def kafka_sasl_options(bootstrap: str, username: str, password: str) -> dict:
    """Kafka connection options for MSK SASL_SSL with SCRAM-SHA-512."""
    return {
        "kafka.bootstrap.servers": bootstrap,
        "kafka.security.protocol": "SASL_SSL",
        "kafka.sasl.mechanism": "SCRAM-SHA-512",
        "kafka.sasl.jaas.config": (
            f"{SCRAM_LOGIN_MODULE} required "
            f'username="{_jaas_escape(username)}" password="{_jaas_escape(password)}";'
        ),
    }


def _jaas_escape(value: str) -> str:
    return value.replace("\\", "\\\\").replace('"', '\\"')


def as_interval(value: str) -> str:
    """Accept '300' (seconds) or any Spark interval string such as '5 minutes'."""
    value = str(value).strip()
    return f"{value} seconds" if value.isdigit() else value


def parse_args(argv: Optional[Sequence[str]] = None) -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Pattern 03-udf: scalar UDF scoring on Spark Real-Time Mode")
    parser.add_argument("--secret-scope", default=SECRET_SCOPE)
    parser.add_argument("--input-topic", default="rtmpack.03-udf.input")
    parser.add_argument("--output-topic", default="rtmpack.03-udf.out.rtm")
    parser.add_argument(
        "--partitions",
        type=int,
        default=None,
        help="Topic partition count. Accepted for the common job interface; the stateless query has no shuffle, so it is not used.",
    )
    parser.add_argument(
        "--checkpoint-interval",
        default="5 minutes",
        help="Real-time trigger interval; progress is checkpointed at this interval (contract section 8: 5 minutes).",
    )
    parser.add_argument(
        "--checkpoint-location",
        required=True,
        help="Checkpoint path, for example /Volumes/<catalog>/<schema>/<volume>/checkpoints/03-udf-rtm.",
    )
    parser.add_argument(
        "--kafka-linger-ms",
        type=int,
        default=None,
        help="Kafka producer linger.ms for the sink. Unset keeps the connector default.",
    )
    parser.add_argument(
        "--scoring",
        choices=["arrow-udf", "builtin"],
        default="arrow-udf",
        help="arrow-udf (the pattern definition: Arrow-optimized Python UDF) or builtin "
        "(the same function with built-in SQL functions, for the variant run).",
    )
    parser.add_argument(
        "--arrow-max-records-per-batch",
        type=int,
        default=ARROW_MAX_RECORDS_PER_BATCH,
        help="spark.sql.execution.arrow.maxRecordsPerBatch. Default 1, as the RTM reference recommends for "
        "DBR 18 and below. 0 leaves the runtime default, for DBR 19+ where rows are batched by time.",
    )
    args = parser.parse_args(argv)
    if args.kafka_linger_ms is not None and args.kafka_linger_ms < 0:
        parser.error("--kafka-linger-ms must be >= 0")
    if args.arrow_max_records_per_batch < 0:
        parser.error("--arrow-max-records-per-batch must be >= 0")
    args.checkpoint_interval = as_interval(args.checkpoint_interval)
    return args


def configure_arrow(spark: SparkSession, max_records_per_batch: int = ARROW_MAX_RECORDS_PER_BATCH) -> None:
    """Sets the Arrow batch size. The default of one record per batch is what the RTM reference
    recommends for DBR 18 and below; 0 keeps the runtime default (time-based batching on DBR 19+)."""
    if max_records_per_batch > 0:
        spark.conf.set("spark.sql.execution.arrow.maxRecordsPerBatch", str(max_records_per_batch))


def run(argv: Optional[Sequence[str]] = None) -> None:
    args = parse_args(argv)

    spark = SparkSession.builder.getOrCreate()
    configure_arrow(spark, args.arrow_max_records_per_batch)

    from pyspark.dbutils import DBUtils  # available on Databricks runtimes

    dbutils = DBUtils(spark)
    bootstrap = dbutils.secrets.get(scope=args.secret_scope, key="kafka-bootstrap")
    username = dbutils.secrets.get(scope=args.secret_scope, key="kafka-username")
    password = dbutils.secrets.get(scope=args.secret_scope, key="kafka-password")
    sasl = kafka_sasl_options(bootstrap, username, password)

    raw = (
        spark.readStream.format("kafka")
        .options(**sasl)
        .option("subscribe", args.input_topic)
        .option("startingOffsets", "latest")
        .load()
    )

    out = score_events(raw, engine=ENGINE, score_udf=builtin_score if args.scoring == "builtin" else None)

    sink_options = dict(sasl)
    if args.kafka_linger_ms is not None:
        sink_options["kafka.linger.ms"] = str(args.kafka_linger_ms)

    query = (
        out.writeStream.format("kafka")
        .options(**sink_options)
        .option("topic", args.output_topic)
        .option("checkpointLocation", args.checkpoint_location)
        .outputMode("update")
        .trigger(realTime=args.checkpoint_interval)
        .start()
    )
    query.awaitTermination()


if __name__ == "__main__":
    run(sys.argv[1:])
