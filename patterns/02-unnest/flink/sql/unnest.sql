-- Pattern 02-unnest: array unnest on Flink SQL.
--
-- Source:  rtmpack.02-unnest.input            (patterns/02-unnest/README.md, Benchmark setup)
-- Sink:    rtmpack.02-unnest.out.flink        (key <order_id>:<item_idx>)
--
-- Values in the form dollar-brace name are substituted before execution from
-- Managed Flink runtime properties (groups kafka and app), CLI arguments or
-- environment variables. See UnnestSql and JobConfig.
--
-- Statements are separated by semicolons. Whole-line comments are removed
-- before the script is split.

SET 'pipeline.name' = 'rtmpack-flink-02-unnest';
SET 'table.local-time-zone' = 'UTC';

CREATE TABLE unnest_input (
  order_id  STRING,
  `key`     STRING,
  event_ts  BIGINT,
  items     ARRAY<ROW<sku STRING, qty INT, price DOUBLE>>,
  payload   STRING,
  in_ts_ltz TIMESTAMP_LTZ(3) METADATA FROM 'timestamp'
) WITH (
  'connector' = 'kafka',
  'topic' = '${app.input.topic}',
  'properties.bootstrap.servers' = '${kafka.bootstrap.servers}',
  'properties.security.protocol' = 'SASL_SSL',
  'properties.sasl.mechanism' = 'SCRAM-SHA-512',
  'properties.sasl.jaas.config' = '${kafka.jaas.config}',
  'scan.startup.mode' = 'latest-offset',
  'format' = 'json'
);

-- kafka_key is the record key. It is listed in key.fields and removed from the
-- JSON value by value.fields-include = EXCEPT_KEY.
CREATE TABLE unnest_output (
  kafka_key STRING,
  order_id  STRING,
  `key`     STRING,
  event_ts  BIGINT,
  item_idx  INT,
  sku       STRING,
  qty       INT,
  price     DOUBLE,
  in_ts     BIGINT,
  engine    STRING
) WITH (
  'connector' = 'kafka',
  'topic' = '${app.output.topic}',
  'properties.bootstrap.servers' = '${kafka.bootstrap.servers}',
  'properties.security.protocol' = 'SASL_SSL',
  'properties.sasl.mechanism' = 'SCRAM-SHA-512',
  'properties.sasl.jaas.config' = '${kafka.jaas.config}',
  'key.format' = 'raw',
  'key.fields' = 'kafka_key',
  'value.format' = 'json',
  'value.fields-include' = 'EXCEPT_KEY',
  'sink.delivery-guarantee' = 'at-least-once',
  'properties.linger.ms' = '0'
);

-- One output row per array element. ORDINALITY is 1-based, so item_idx is
-- ord - 1 to give the 0-based position the contract requires.
--
-- in_ts is the broker timestamp of the input record, converted to epoch
-- milliseconds in UTC (the session time zone is pinned above).
INSERT INTO unnest_output
SELECT
  CONCAT(o.order_id, ':', CAST(t.ord - 1 AS STRING)) AS kafka_key,
  o.order_id,
  o.`key`,
  o.event_ts,
  t.ord - 1 AS item_idx,
  t.sku,
  t.qty,
  t.price,
  UNIX_TIMESTAMP(CAST(o.in_ts_ltz AS STRING), 'yyyy-MM-dd HH:mm:ss.SSS') * 1000
    + EXTRACT(MILLISECOND FROM o.in_ts_ltz) AS in_ts,
  'flink' AS engine
FROM unnest_input AS o
CROSS JOIN UNNEST(o.items) WITH ORDINALITY AS t(sku, qty, price, ord);
