"""Pattern 01-dedup: deduplication (keep first per event_id) on Spark Real-Time Mode.

Reads the contract input topic from Amazon MSK (SASL_SSL, SCRAM-SHA-512), keeps the
first record per event_id inside the state horizon, and writes the contract output
topic with key = event_id and a JSON value.

Runs as a Databricks Python task on a dedicated classic cluster with
spark.databricks.streaming.realTimeMode.enabled=true. The transformation is a pure
function (dedup_first_per_event) so it can be unit tested in batch mode.
"""

import argparse
import sys
from typing import Optional, Sequence

from pyspark.sql import DataFrame, SparkSession
from pyspark.sql import functions as F
from pyspark.sql.types import BooleanType, LongType, StringType, StructField, StructType

ENGINE = "rtm"
SECRET_SCOPE = "rtmpack"
SCRAM_LOGIN_MODULE = "kafkashaded.org.apache.kafka.common.security.scram.ScramLoginModule"

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


def dedup_first_per_event(
    raw: DataFrame, state_horizon: str = "60 seconds", engine: str = ENGINE,
    operator: str = "drop-duplicates",
) -> DataFrame:
    """Keep the first record per event_id within the state horizon.

    `raw` has the Kafka source schema (key, value, topic, partition, offset,
    timestamp, timestampType). Returns a DataFrame with two columns:
      key   - the Kafka record key, event_id (UTF-8 string)
      value - the output JSON: the input event fields, plus in_ts and engine
    """
    parsed = (
        raw.select(
            F.from_json(F.col("value").cast("string"), INPUT_EVENT_SCHEMA).alias("e"),
            F.unix_millis(F.col("timestamp")).alias("in_ts"),
        )
        .select("e.*", "in_ts")
        .withColumn("event_time", F.timestamp_millis(F.col("event_ts")))
        .filter(F.col("event_id").isNotNull() & F.col("event_time").isNotNull())
    )

    if parsed.isStreaming and operator == "within-watermark":
        # Checks whether a newer runtime accepts the operator the RTM reference lists.
        deduped = parsed.withWatermark("event_time", state_horizon).dropDuplicatesWithinWatermark(["event_id"])
    elif parsed.isStreaming:
        # DBR 18.2 rejects dropDuplicatesWithinWatermark in real-time mode. The supported
        # form is dropDuplicates with the event-time column in the key, so the watermark
        # can evict state.
        # A duplicate carries the same event_ts as its original, so keying on
        # (event_id, event_time) keeps exactly one row per event_id while the
        # duplicate delay stays below the state horizon.
        deduped = parsed.withWatermark("event_time", state_horizon).dropDuplicates(
            ["event_id", "event_time"]
        )
    else:
        # Batch input (unit tests): plain dropDuplicates on the same key.
        deduped = parsed.dropDuplicates(["event_id"])

    return deduped.select(
        F.col("event_id").alias("key"),
        F.to_json(
            F.struct(
                F.col("event_id"),
                F.col("key"),
                F.col("seq"),
                F.col("event_ts"),
                F.col("is_dup"),
                F.col("payload"),
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
    """Accept '60' (seconds) or any Spark interval string such as '60 seconds'."""
    value = str(value).strip()
    return f"{value} seconds" if value.isdigit() else value


def parse_args(argv: Optional[Sequence[str]] = None) -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Pattern 01-dedup: dedup on Spark Real-Time Mode")
    parser.add_argument("--secret-scope", default=SECRET_SCOPE)
    parser.add_argument("--input-topic", default="rtmpack.01-dedup.input")
    parser.add_argument("--output-topic", default="rtmpack.01-dedup.out.rtm")
    parser.add_argument(
        "--partitions",
        type=int,
        default=8,
        help="Shuffle partitions. Kafka topic partitions must match (contract: 8 by default).",
    )
    parser.add_argument(
        "--state-horizon",
        default="60 seconds",
        help="Watermark delay for dedup state (contract: 60 seconds).",
    )
    parser.add_argument(
        "--checkpoint-interval",
        default="60",
        help="Real-time trigger interval; progress is checkpointed at this interval (contract: 60 s).",
    )
    parser.add_argument(
        "--kafka-linger-ms",
        type=int,
        default=None,
        help="Kafka producer linger.ms for the sink. Unset keeps the connector default.",
    )
    parser.add_argument(
        "--dedup-operator",
        choices=["drop-duplicates", "within-watermark"],
        default="drop-duplicates",
        help="drop-duplicates: dropDuplicates on (event_id, event_time), the pattern definition. "
        "within-watermark: dropDuplicatesWithinWatermark(event_id), to check whether a runtime accepts it in real-time mode.",
    )
    parser.add_argument(
        "--checkpoint-location",
        required=True,
        help="Checkpoint path, for example /Volumes/<catalog>/<schema>/<volume>/checkpoints/01-dedup-rtm.",
    )
    args = parser.parse_args(argv)
    if args.partitions < 1:
        parser.error("--partitions must be >= 1")
    if args.kafka_linger_ms is not None and args.kafka_linger_ms < 0:
        parser.error("--kafka-linger-ms must be >= 0")
    args.state_horizon = as_interval(args.state_horizon)
    args.checkpoint_interval = as_interval(args.checkpoint_interval)
    return args


def run(argv: Optional[Sequence[str]] = None) -> None:
    args = parse_args(argv)

    spark = SparkSession.builder.getOrCreate()
    # Shuffle partitions drive the slot requirement; the cluster spark_conf sets the same value.
    spark.conf.set("spark.sql.shuffle.partitions", str(args.partitions))

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

    out = dedup_first_per_event(raw, state_horizon=args.state_horizon, engine=ENGINE, operator=args.dedup_operator)

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
