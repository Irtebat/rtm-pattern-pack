"""Pattern 06-tumbling: tumbling 60 s count and sum per key on Spark Real-Time Mode.

Reads the contract input topic from Amazon MSK (SASL_SSL, SCRAM-SHA-512), aggregates
count, sum(amount) and max(in_ts) per key and window on event time, and writes
the contract output topic with key "<key>:<window_start_ms>" and a JSON value.

Runs as a Databricks Python task on a dedicated classic cluster with
spark.databricks.streaming.realTimeMode.enabled=true. The transformation is a pure
function (aggregate_tumbling) so it can be unit tested in batch mode.
"""

import argparse
import sys
from typing import Optional, Sequence

from pyspark.sql import DataFrame, SparkSession
from pyspark.sql import functions as F
from pyspark.sql.types import LongType, StringType, StructField, StructType

ENGINE = "rtm"
SECRET_SCOPE = "rtmpack"
SCRAM_LOGIN_MODULE = "kafkashaded.org.apache.kafka.common.security.scram.ScramLoginModule"
WINDOW_DURATION = "60 seconds"

# Contract section 2 and patterns/06-tumbling/README.md, Benchmark setup: input event JSON value (dup_ratio = 0, plus amount).
INPUT_EVENT_SCHEMA = StructType(
    [
        StructField("event_id", StringType(), True),
        StructField("key", StringType(), True),
        StructField("seq", LongType(), True),
        StructField("event_ts", LongType(), True),  # epoch milliseconds
        StructField("amount", LongType(), True),  # integer cents
        StructField("payload", StringType(), True),
    ]
)


def aggregate_tumbling(
    raw: DataFrame,
    watermark: str = "10 seconds",
    window_duration: str = WINDOW_DURATION,
    engine: str = ENGINE,
) -> DataFrame:
    """Per key and 60-second tumbling window: count, sum(amount), max(in_ts).

    `raw` has the Kafka source schema (key, value, topic, partition, offset,
    timestamp, timestampType). Returns a DataFrame with two columns:
      key   - the Kafka record key, "<key>:<window_start_ms>" (UTF-8 string)
      value - the output JSON: key, window_start_ms, window_end_ms, count,
              sum_amount, in_ts (largest input in_ts in the window so far), engine
    """
    parsed = (
        raw.select(
            F.from_json(F.col("value").cast("string"), INPUT_EVENT_SCHEMA).alias("e"),
            F.unix_millis(F.col("timestamp")).alias("in_ts"),
        )
        .select("e.*", "in_ts")
        .withColumn("event_time", F.timestamp_millis(F.col("event_ts")))
        .filter(F.col("key").isNotNull() & F.col("event_time").isNotNull())
    )

    # The watermark bounds window state: a window is dropped once the watermark passes
    # its end, and input later than the watermark is discarded.
    aggregated = (
        parsed.withWatermark("event_time", watermark)
        .groupBy(F.window(F.col("event_time"), window_duration), F.col("key"))
        .agg(
            F.count(F.lit(1)).alias("count"),
            F.sum("amount").alias("sum_amount"),
            F.max("in_ts").alias("in_ts"),
        )
        .select(
            F.col("key"),
            F.unix_millis(F.col("window.start")).alias("window_start_ms"),
            F.unix_millis(F.col("window.end")).alias("window_end_ms"),
            F.col("count"),
            F.col("sum_amount"),
            F.col("in_ts"),
        )
    )

    return aggregated.select(
        F.concat(F.col("key"), F.lit(":"), F.col("window_start_ms").cast("string")).alias("key"),
        F.to_json(
            F.struct(
                F.col("key"),
                F.col("window_start_ms"),
                F.col("window_end_ms"),
                F.col("count"),
                F.col("sum_amount"),
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
    parser = argparse.ArgumentParser(description="Pattern 06-tumbling: tumbling 60 s count and sum per key on Spark Real-Time Mode")
    parser.add_argument("--secret-scope", default=SECRET_SCOPE)
    parser.add_argument("--input-topic", default="rtmpack.06-tumbling.input")
    parser.add_argument("--output-topic", default="rtmpack.06-tumbling.out.rtm")
    parser.add_argument(
        "--partitions",
        type=int,
        default=4,
        help=(
            "Shuffle partitions for the aggregation (contract sizing: 4). The Kafka input topic "
            "has 8 partitions, which sets the number of source tasks."
        ),
    )
    parser.add_argument(
        "--state-horizon",
        default="10 seconds",
        help="Watermark delay for window state (contract: 10 seconds).",
    )
    parser.add_argument(
        "--checkpoint-interval",
        default="60",
        help="Real-time trigger interval; progress is checkpointed at this interval.",
    )
    parser.add_argument(
        "--kafka-linger-ms",
        type=int,
        default=None,
        help="Kafka producer linger.ms for the sink. Unset keeps the connector default.",
    )
    parser.add_argument(
        "--checkpoint-location",
        required=True,
        help="Checkpoint path, for example /Volumes/<catalog>/<schema>/<volume>/checkpoints/06-tumbling-rtm.",
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

    out = aggregate_tumbling(raw, watermark=args.state_horizon, engine=ENGINE)

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
