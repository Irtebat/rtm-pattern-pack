"""Pattern 05-lookup: lookup join with a small reference table on Spark Real-Time Mode.

Reads the contract input topic from Amazon MSK (SASL_SSL, SCRAM-SHA-512), joins each event
with the merchant reference table and writes the contract output topic with key = event_id
and a JSON value. The reference table is a CSV file in a Unity Catalog volume. It is read
once with spark.read (explicit schema) and joined as a broadcast, inner stream-static join on
merchant_id. Events with an unknown merchant_id produce no output.

Runs as a Databricks Python task on a dedicated classic cluster with
spark.databricks.streaming.realTimeMode.enabled=true. The transformation is a pure function
(enrich_events) so it can be unit tested in batch mode. The broadcast join adds no shuffle
stage, so tasks = Kafka input partitions.
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

# Contract section 2 and patterns/05-lookup/README.md, Benchmark setup: input event JSON value.
INPUT_EVENT_SCHEMA = StructType(
    [
        StructField("event_id", StringType(), True),
        StructField("key", StringType(), True),
        StructField("seq", LongType(), True),
        StructField("event_ts", LongType(), True),  # epoch milliseconds
        StructField("is_dup", BooleanType(), True),
        StructField("merchant_id", StringType(), True),
        StructField("payload", StringType(), True),
    ]
)

# Reference table (patterns/05-lookup/README.md, Benchmark setup):, header merchant_id,risk_tier,category,country.
MERCHANT_SCHEMA = StructType(
    [
        StructField("merchant_id", StringType(), False),
        StructField("risk_tier", StringType(), True),
        StructField("category", StringType(), True),
        StructField("country", StringType(), True),
    ]
)


def read_merchants(spark: SparkSession, path: str) -> DataFrame:
    """Read the reference CSV with an explicit schema. No schema inference, header required."""
    return spark.read.schema(MERCHANT_SCHEMA).option("header", "true").csv(path)


def enrich_events(raw: DataFrame, merchants: DataFrame, engine: str = ENGINE) -> DataFrame:
    """Inner stream-static broadcast join of events with the merchant table.

    `raw` has the Kafka source schema (key, value, topic, partition, offset,
    timestamp, timestampType). Returns a DataFrame with two columns:
      key   - event_id
      value - the output JSON: event_id, key, event_ts, merchant_id, risk_tier,
              category, country, in_ts, engine
    Rows with a null event_id or merchant_id, or a merchant_id that is not in the table,
    produce no output (inner join).
    """
    parsed = raw.select(
        F.from_json(F.col("value").cast("string"), INPUT_EVENT_SCHEMA).alias("e"),
        F.unix_millis(F.col("timestamp")).alias("in_ts"),
    ).filter(F.col("e.event_id").isNotNull())

    events = parsed.select(
        F.col("e.event_id").alias("event_id"),
        F.col("e.key").alias("key"),
        F.col("e.event_ts").alias("event_ts"),
        F.col("e.merchant_id").alias("merchant_id"),
        F.col("in_ts"),
    )

    joined = events.join(F.broadcast(merchants), on="merchant_id", how="inner")

    return joined.select(
        F.col("event_id").alias("key"),
        F.to_json(
            F.struct(
                F.col("event_id"),
                F.col("key"),
                F.col("event_ts"),
                F.col("merchant_id"),
                F.col("risk_tier"),
                F.col("category"),
                F.col("country"),
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
    parser = argparse.ArgumentParser(description="Pattern 05-lookup: lookup join on Spark Real-Time Mode")
    parser.add_argument("--secret-scope", default=SECRET_SCOPE)
    parser.add_argument("--input-topic", default="rtmpack.05-lookup.input")
    parser.add_argument("--output-topic", default="rtmpack.05-lookup.out.rtm")
    parser.add_argument(
        "--checkpoint-interval",
        default="5 minutes",
        help="Real-time trigger interval; progress is checkpointed at this interval (contract section 8: 5 minutes).",
    )
    parser.add_argument(
        "--checkpoint-location",
        required=True,
        help="Checkpoint path, for example /Volumes/<catalog>/<schema>/<volume>/checkpoints/05-lookup-rtm.",
    )
    parser.add_argument(
        "--kafka-linger-ms",
        type=int,
        default=None,
        help="Kafka producer linger.ms for the sink. Unset keeps the connector default.",
    )
    parser.add_argument(
        "--reference-path",
        required=True,
        help="Unity Catalog volume path of merchants.csv, for example "
        "/Volumes/<catalog>/<schema>/<volume>/refdata/merchants.csv.",
    )
    parser.add_argument(
        "--reference-mode",
        choices=["read", "cached", "local"],
        default="read",
        help="How the static side is held. read: a file scan in the plan (pattern definition). "
        "cached: the same DataFrame cached in memory before the query starts. local: the rows "
        "collected to the driver once and embedded in the plan as a local relation, so no file "
        "is read again. The last two are variants.",
    )
    args = parser.parse_args(argv)
    if args.kafka_linger_ms is not None and args.kafka_linger_ms < 0:
        parser.error("--kafka-linger-ms must be >= 0")
    args.checkpoint_interval = as_interval(args.checkpoint_interval)
    return args


def prepare_reference(spark: SparkSession, merchants: DataFrame, mode: str) -> DataFrame:
    """Returns the static side for the join according to --reference-mode."""
    if mode == "cached":
        merchants = merchants.cache()
        merchants.count()  # materialize before the stream starts
        return merchants
    if mode == "local":
        return spark.createDataFrame(merchants.collect(), schema=merchants.schema)
    return merchants


def run(argv: Optional[Sequence[str]] = None) -> None:
    args = parse_args(argv)

    spark = SparkSession.builder.getOrCreate()

    from pyspark.dbutils import DBUtils  # available on Databricks runtimes

    dbutils = DBUtils(spark)
    bootstrap = dbutils.secrets.get(scope=args.secret_scope, key="kafka-bootstrap")
    username = dbutils.secrets.get(scope=args.secret_scope, key="kafka-username")
    password = dbutils.secrets.get(scope=args.secret_scope, key="kafka-password")
    sasl = kafka_sasl_options(bootstrap, username, password)

    merchants = prepare_reference(spark, read_merchants(spark, args.reference_path), args.reference_mode)

    raw = (
        spark.readStream.format("kafka")
        .options(**sasl)
        .option("subscribe", args.input_topic)
        .option("startingOffsets", "latest")
        .load()
    )

    out = enrich_events(raw, merchants, engine=ENGINE)

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
