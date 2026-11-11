"""Pattern 08-join: inner stream-stream join (orders and payments) on Spark Real-Time Mode.

Reads <input-topic>.orders and <input-topic>.payments from Amazon MSK (SASL_SSL,
SCRAM-SHA-512), joins a payment to its order on order_id when the payment event time
is between the order event time and the order event time plus 30 seconds, and writes
the contract output topic with key = order_id and a JSON value.

Runs as a Databricks Python task on a dedicated classic cluster with
spark.databricks.streaming.realTimeMode.enabled=true. The transformation is a pure
function (join_orders_payments) so it can be unit tested in batch mode.
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

# Settings from the RTM setup page that stream-stream joins need. They are applied to the
# session by apply_join_confs. Some of them may only take effect as cluster Spark conf;
# The Terraform spark_conf sets the same values on the job cluster; keep this list in
# sync with it.
STREAM_JOIN_CONFS = {
    "spark.databricks.streaming.realTimeMode.streamStreamJoin.enabled": "true",
    "spark.sql.streaming.join.stateFormatVersion": "4",
    "spark.sql.streaming.join.stateFormatV4.enabled": "true",
    "spark.sql.streaming.stateStore.rocksdb.mergeOperatorVersion": "2",
    "spark.sql.streaming.realTimeMode.controlMessage.enabled": "true",
}

# Input event JSON values, see patterns/08-join/README.md, Benchmark setup.
ORDER_SCHEMA = StructType(
    [
        StructField("order_id", StringType(), True),
        StructField("key", StringType(), True),
        StructField("event_ts", LongType(), True),  # epoch milliseconds
        StructField("amount", LongType(), True),
        StructField("payload", StringType(), True),
    ]
)

PAYMENT_SCHEMA = StructType(
    [
        StructField("payment_id", StringType(), True),
        StructField("order_id", StringType(), True),
        StructField("event_ts", LongType(), True),  # epoch milliseconds
        StructField("method", StringType(), True),
        StructField("payload", StringType(), True),
    ]
)


def _parse(raw: DataFrame, schema: StructType) -> DataFrame:
    """Parse the Kafka value, add in_ts (Kafka timestamp, epoch ms) and event_time."""
    return (
        raw.select(
            F.from_json(F.col("value").cast("string"), schema).alias("e"),
            F.unix_millis(F.col("timestamp")).alias("in_ts"),
        )
        .select("e.*", "in_ts")
        .withColumn("event_time", F.timestamp_millis(F.col("event_ts")))
        .filter(F.col("order_id").isNotNull() & F.col("event_time").isNotNull())
    )


def join_orders_payments(
    orders_raw: DataFrame,
    payments_raw: DataFrame,
    watermark: str = "30 seconds",
    join_window: str = "30 seconds",
    engine: str = ENGINE,
) -> DataFrame:
    """Inner join of orders and payments on order_id within [0, join_window] after the order.

    Both inputs have the Kafka source schema (key, value, topic, partition, offset,
    timestamp, timestampType). Returns a DataFrame with two columns:
      key   - the Kafka record key, order_id (UTF-8 string)
      value - the output JSON: order_id, payment_id, key, amount, method, order_ts,
              payment_ts, in_ts (the larger of the two input in_ts values) and engine
    """
    orders = (
        _parse(orders_raw, ORDER_SCHEMA)
        .select(
            F.col("order_id"),
            F.col("key").alias("cust_key"),
            F.col("event_ts").alias("order_ts"),
            F.col("amount"),
            F.col("in_ts").alias("o_in_ts"),
            F.col("event_time").alias("o_time"),
        )
        .withWatermark("o_time", watermark)
    )
    payments = (
        _parse(payments_raw, PAYMENT_SCHEMA)
        .select(
            F.col("payment_id"),
            F.col("order_id").alias("p_order_id"),
            F.col("event_ts").alias("payment_ts"),
            F.col("method"),
            F.col("in_ts").alias("p_in_ts"),
            F.col("event_time").alias("p_time"),
        )
        .withWatermark("p_time", watermark)
    )

    condition = (
        (F.col("order_id") == F.col("p_order_id"))
        & (F.col("p_time") >= F.col("o_time"))
        & (F.col("p_time") <= F.col("o_time") + F.expr(f"interval {join_window}"))
    )
    joined = orders.join(payments, condition, "inner")

    return joined.select(
        F.col("order_id").alias("key"),
        F.to_json(
            F.struct(
                F.col("order_id"),
                F.col("payment_id"),
                F.col("cust_key").alias("key"),
                F.col("amount"),
                F.col("method"),
                F.col("order_ts"),
                F.col("payment_ts"),
                F.greatest(F.col("o_in_ts"), F.col("p_in_ts")).alias("in_ts"),
                F.lit(engine).alias("engine"),
            )
        ).alias("value"),
    )


def apply_join_confs(spark: SparkSession) -> None:
    """Set the stream-stream join settings on the session and report any that are rejected.

    A setting that cannot be changed at runtime is reported on stderr and must then be set
    in the cluster spark_conf instead.
    """
    for name, value in STREAM_JOIN_CONFS.items():
        try:
            spark.conf.set(name, value)
        except Exception as exc:  # noqa: BLE001 - report every rejection, keep going
            print(f"WARNING: could not set {name}={value} on the session: {exc}", file=sys.stderr)


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
    """Accept '30' (seconds) or any Spark interval string such as '30 seconds'."""
    value = str(value).strip()
    return f"{value} seconds" if value.isdigit() else value


def parse_args(argv: Optional[Sequence[str]] = None) -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Pattern 08-join: inner stream-stream join on Spark Real-Time Mode")
    parser.add_argument("--secret-scope", default=SECRET_SCOPE)
    parser.add_argument(
        "--input-topic",
        default="rtmpack.08-join.input",
        help="Input topic base. The job reads <base>.orders and <base>.payments.",
    )
    parser.add_argument("--output-topic", default="rtmpack.08-join.out.rtm")
    parser.add_argument(
        "--partitions",
        type=int,
        default=4,
        help="Shuffle partitions for the join stage (contract: 4). Source tasks follow the topic partitions.",
    )
    parser.add_argument(
        "--watermark-delay",
        default="30 seconds",
        help="Watermark delay on both inputs (contract: 30 seconds).",
    )
    parser.add_argument(
        "--join-window",
        default="30 seconds",
        help="A payment matches an order up to this long after the order event time (contract: 30 seconds).",
    )
    parser.add_argument(
        "--output-mode",
        choices=["update", "append"],
        default="update",
        help="Sink output mode. RTM documents update mode only, but Structured Streaming rejects "
        "stream-stream joins in update mode; append is the alternative.",
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
        help="Checkpoint path, for example /Volumes/<catalog>/<schema>/<volume>/checkpoints/08-join-rtm.",
    )
    args = parser.parse_args(argv)
    if args.partitions < 1:
        parser.error("--partitions must be >= 1")
    if args.kafka_linger_ms is not None and args.kafka_linger_ms < 0:
        parser.error("--kafka-linger-ms must be >= 0")
    args.watermark_delay = as_interval(args.watermark_delay)
    args.join_window = as_interval(args.join_window)
    args.checkpoint_interval = as_interval(args.checkpoint_interval)
    return args


def run(argv: Optional[Sequence[str]] = None) -> None:
    args = parse_args(argv)

    spark = SparkSession.builder.getOrCreate()
    # Shuffle partitions set the join stage size; the cluster spark_conf sets the same value.
    spark.conf.set("spark.sql.shuffle.partitions", str(args.partitions))
    apply_join_confs(spark)

    from pyspark.dbutils import DBUtils  # available on Databricks runtimes

    dbutils = DBUtils(spark)
    bootstrap = dbutils.secrets.get(scope=args.secret_scope, key="kafka-bootstrap")
    username = dbutils.secrets.get(scope=args.secret_scope, key="kafka-username")
    password = dbutils.secrets.get(scope=args.secret_scope, key="kafka-password")
    sasl = kafka_sasl_options(bootstrap, username, password)

    def source(suffix: str) -> DataFrame:
        return (
            spark.readStream.format("kafka")
            .options(**sasl)
            .option("subscribe", f"{args.input_topic}.{suffix}")
            .option("startingOffsets", "latest")
            .load()
        )

    out = join_orders_payments(
        source("orders"),
        source("payments"),
        watermark=args.watermark_delay,
        join_window=args.join_window,
        engine=ENGINE,
    )

    sink_options = dict(sasl)
    if args.kafka_linger_ms is not None:
        sink_options["kafka.linger.ms"] = str(args.kafka_linger_ms)

    query = (
        out.writeStream.format("kafka")
        .options(**sink_options)
        .option("topic", args.output_topic)
        .option("checkpointLocation", args.checkpoint_location)
        .outputMode(args.output_mode)
        .trigger(realTime=args.checkpoint_interval)
        .start()
    )
    query.awaitTermination()


if __name__ == "__main__":
    run(sys.argv[1:])
