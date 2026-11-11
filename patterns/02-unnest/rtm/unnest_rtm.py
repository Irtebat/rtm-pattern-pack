"""Pattern 02-unnest: array unnest (one output record per array element) on Spark Real-Time Mode.

Reads the contract input topic from Amazon MSK (SASL_SSL, SCRAM-SHA-512), explodes the
`items` array with posexplode, and writes the contract output topic with
key = "<order_id>:<item_idx>" and a JSON value.

Runs as a Databricks Python task on a dedicated classic cluster with
spark.databricks.streaming.realTimeMode.enabled=true. The transformation is a pure
function (unnest_items) so it can be unit tested in batch mode. The query has no
shuffle, so the stage count is one and tasks = Kafka input partitions.
"""

import argparse
import sys
from typing import Optional, Sequence

from pyspark.sql import DataFrame, SparkSession
from pyspark.sql import functions as F
from pyspark.sql.types import (
    ArrayType,
    DoubleType,
    IntegerType,
    LongType,
    StringType,
    StructField,
    StructType,
)

ENGINE = "rtm"
SECRET_SCOPE = "rtmpack"
SCRAM_LOGIN_MODULE = "kafkashaded.org.apache.kafka.common.security.scram.ScramLoginModule"

# Input order JSON value, see patterns/02-unnest/README.md, Benchmark setup.
ITEM_SCHEMA = StructType(
    [
        StructField("sku", StringType(), True),
        StructField("qty", IntegerType(), True),
        StructField("price", DoubleType(), True),
    ]
)
INPUT_ORDER_SCHEMA = StructType(
    [
        StructField("order_id", StringType(), True),
        StructField("key", StringType(), True),
        StructField("event_ts", LongType(), True),  # epoch milliseconds
        StructField("items", ArrayType(ITEM_SCHEMA), True),
        StructField("payload", StringType(), True),
    ]
)


ITEMS_DDL = "array<struct<sku:string,qty:int,price:double>>"
PARSE_MODES = ("from-json", "variant")


def parse_orders(raw: DataFrame, parse: str = "from-json") -> DataFrame:
    """Parse the Kafka value into a struct column `o` with the input order fields, plus `in_ts`.

    from-json: from_json with the contract schema.
    variant:   try_parse_json into a VARIANT, then try_variant_get for each field. The
               same values result; malformed JSON gives a null order and is dropped.
    """
    in_ts = F.unix_millis(F.col("timestamp")).alias("in_ts")
    if parse == "from-json":
        return raw.select(F.from_json(F.col("value").cast("string"), INPUT_ORDER_SCHEMA).alias("o"), in_ts)
    if parse != "variant":
        raise ValueError(f"unknown parse mode: {parse}")
    v = raw.select(F.expr("try_parse_json(CAST(value AS STRING))").alias("v"), in_ts)
    return v.select(
        F.struct(
            F.expr("try_variant_get(v, '$.order_id', 'string')").alias("order_id"),
            F.expr("try_variant_get(v, '$.key', 'string')").alias("key"),
            F.expr("try_variant_get(v, '$.event_ts', 'bigint')").alias("event_ts"),
            F.expr(f"try_variant_get(v, '$.items', '{ITEMS_DDL}')").alias("items"),
        ).alias("o"),
        F.col("in_ts"),
    )


def unnest_items(raw: DataFrame, engine: str = ENGINE, parse: str = "from-json") -> DataFrame:
    """Emit one record per element of `items`.

    `raw` has the Kafka source schema (key, value, topic, partition, offset,
    timestamp, timestampType). Returns a DataFrame with two columns:
      key   - "<order_id>:<item_idx>", item_idx is the 0-based array position
      value - the output JSON: order_id, key, event_ts, item_idx, sku, qty, price,
              in_ts, engine
    Rows with a null order_id, or a null or empty items array, produce no output.
    """
    parsed = parse_orders(raw, parse).filter(F.col("o.order_id").isNotNull())

    exploded = parsed.select(
        F.col("o.order_id").alias("order_id"),
        F.col("o.key").alias("key"),
        F.col("o.event_ts").alias("event_ts"),
        F.col("in_ts"),
        F.posexplode(F.col("o.items")).alias("item_idx", "item"),
    )

    return exploded.select(
        F.concat_ws(":", F.col("order_id"), F.col("item_idx").cast("string")).alias("key"),
        F.to_json(
            F.struct(
                F.col("order_id"),
                F.col("key"),
                F.col("event_ts"),
                F.col("item_idx"),
                F.col("item.sku").alias("sku"),
                F.col("item.qty").alias("qty"),
                F.col("item.price").alias("price"),
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
    parser = argparse.ArgumentParser(description="Pattern 02-unnest: array unnest on Spark Real-Time Mode")
    parser.add_argument("--secret-scope", default=SECRET_SCOPE)
    parser.add_argument("--input-topic", default="rtmpack.02-unnest.input")
    parser.add_argument("--output-topic", default="rtmpack.02-unnest.out.rtm")
    parser.add_argument(
        "--checkpoint-interval",
        default="5 minutes",
        help="Real-time trigger interval; progress is checkpointed at this interval (contract section 8: 5 minutes).",
    )
    parser.add_argument(
        "--checkpoint-location",
        required=True,
        help="Checkpoint path, for example /Volumes/<catalog>/<schema>/<volume>/checkpoints/02-unnest-rtm.",
    )
    parser.add_argument(
        "--kafka-linger-ms",
        type=int,
        default=None,
        help="Kafka producer linger.ms for the sink. Unset keeps the connector default.",
    )
    parser.add_argument(
        "--parse",
        choices=PARSE_MODES,
        default="from-json",
        help="from-json (schema-based parse, the default) or variant (try_parse_json into a VARIANT, "
        "then try_variant_get per field), for the variant run.",
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

    out = unnest_items(raw, engine=ENGINE, parse=args.parse)

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
