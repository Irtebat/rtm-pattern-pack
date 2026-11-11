"""Pattern 04-routing: demux and fan-out on Spark Real-Time Mode.

Reads the contract input topic from Amazon MSK (SASL_SSL, SCRAM-SHA-512) and writes every
event twice from one query:

  <base>.<type>  the type topic (demux): payment, order, click or login
  <base>.all     the all-events topic (fan-out)

where <base> is --output-topic (default rtmpack.04-routing.out.rtm). Each event becomes two rows
with explode(array(type_topic, all_topic)) into a `topic` column. The Kafka sink is started
WITHOUT the "topic" option, so each row is written to the topic named in its `topic` column.
Key = event_id; the value is the input fields without `payload`, plus in_ts and engine.

Runs as a Databricks Python task on a dedicated classic cluster with
spark.databricks.streaming.realTimeMode.enabled=true. The transformation (route_events) is a
pure function so it can be unit tested in batch mode. The query has no shuffle, so the stage
count is one and tasks = Kafka input partitions.
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

# patterns/04-routing/README.md, Benchmark setup: the four event types, and the suffix of the all-events topic.
EVENT_TYPES = ("payment", "order", "click", "login")
ALL_SUFFIX = "all"

# Contract section 2 and patterns/04-routing/README.md, Benchmark setup: input event JSON value.
INPUT_EVENT_SCHEMA = StructType(
    [
        StructField("event_id", StringType(), True),
        StructField("key", StringType(), True),
        StructField("seq", LongType(), True),
        StructField("event_ts", LongType(), True),  # epoch milliseconds
        StructField("is_dup", BooleanType(), True),
        StructField("type", StringType(), True),
        StructField("payload", StringType(), True),
    ]
)


def route_events(raw: DataFrame, output_base: str, engine: str = ENGINE, mode: str = "demux-fanout") -> DataFrame:
    """Emit two rows per event: one for its type topic, one for the all-events topic.

    `raw` has the Kafka source schema (key, value, topic, partition, offset,
    timestamp, timestampType). Returns a DataFrame with three columns:
      topic - "<output_base>.<type>" or "<output_base>.all"
      key   - event_id
      value - the output JSON: event_id, key, seq, event_ts, is_dup, type, in_ts, engine
    Rows with a null event_id (including malformed JSON) produce no output. An event whose
    type is not one of the four known types is written to the all-events topic only, so no
    topic name is ever built from an unexpected value.
    """
    parsed = raw.select(
        F.from_json(F.col("value").cast("string"), INPUT_EVENT_SCHEMA).alias("e"),
        F.unix_millis(F.col("timestamp")).alias("in_ts"),
    ).filter(F.col("e.event_id").isNotNull())

    all_topic = F.lit(f"{output_base}.{ALL_SUFFIX}")
    type_topic = F.concat(F.lit(f"{output_base}."), F.col("e.type"))
    known_type = F.col("e.type").isin(*EVENT_TYPES)

    exploded = parsed.select(
        F.col("e.event_id").alias("event_id"),
        F.col("e.key").alias("key"),
        F.col("e.seq").alias("seq"),
        F.col("e.event_ts").alias("event_ts"),
        F.col("e.is_dup").alias("is_dup"),
        F.col("e.type").alias("type"),
        F.col("in_ts"),
        (
            # Diagnostic variant (see the pattern README): one row per event, still routed
            # through the topic column, to separate the cost of the topic column from the
            # cost of fanning out to five topics.
            all_topic
            if mode in ("all-only", "all-fixed")
            else F.explode(F.when(known_type, F.array(type_topic, all_topic)).otherwise(F.array(all_topic)))
        ).alias("topic"),
    )

    return exploded.select(
        F.col("topic"),
        F.col("event_id").alias("key"),
        F.to_json(
            F.struct(
                F.col("event_id"),
                F.col("key"),
                F.col("seq"),
                F.col("event_ts"),
                F.col("is_dup"),
                F.col("type"),
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
    parser = argparse.ArgumentParser(description="Pattern 04-routing: demux and fan-out on Spark Real-Time Mode")
    parser.add_argument("--secret-scope", default=SECRET_SCOPE)
    parser.add_argument("--input-topic", default="rtmpack.04-routing.input")
    parser.add_argument(
        "--output-topic",
        default="rtmpack.04-routing.out.rtm",
        help="Output topic base. Records go to <base>.<type> and <base>.all.",
    )
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
        help="Checkpoint path, for example /Volumes/<catalog>/<schema>/<volume>/checkpoints/04-routing-rtm.",
    )
    parser.add_argument(
        "--kafka-linger-ms",
        type=int,
        default=None,
        help="Kafka producer linger.ms for the sink. Unset keeps the connector default.",
    )
    parser.add_argument(
        "--mode",
        choices=["demux-fanout", "all-only", "all-fixed"],
        default="demux-fanout",
        help="demux-fanout: the pattern (type topic plus all-events topic). all-only: diagnostic "
        "variant writing each event once to the all-events topic through the topic column. "
        "all-fixed: the same single output, written with the sink's fixed topic option and no "
        "topic column (diagnostic for the cost of the topic column).",
    )
    args = parser.parse_args(argv)
    if args.kafka_linger_ms is not None and args.kafka_linger_ms < 0:
        parser.error("--kafka-linger-ms must be >= 0")
    args.checkpoint_interval = as_interval(args.checkpoint_interval)
    return args


def run(argv: Optional[Sequence[str]] = None) -> None:
    args = parse_args(argv)

    spark = SparkSession.builder.getOrCreate()

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

    out = route_events(raw, args.output_topic, engine=ENGINE, mode=args.mode)

    sink_options = dict(sasl)
    if args.kafka_linger_ms is not None:
        sink_options["kafka.linger.ms"] = str(args.kafka_linger_ms)

    # No "topic" option: the sink writes each row to the topic in its `topic` column.
    # all-fixed drops the column and names the topic once, as a stateless single-topic job does.
    if args.mode == "all-fixed":
        out = out.drop("topic")
        sink_options["topic"] = f"{args.output_topic}.{ALL_SUFFIX}"
    query = (
        out.writeStream.format("kafka")
        .options(**sink_options)
        .option("checkpointLocation", args.checkpoint_location)
        .outputMode("update")
        .trigger(realTime=args.checkpoint_interval)
        .start()
    )
    query.awaitTermination()


if __name__ == "__main__":
    run(sys.argv[1:])
