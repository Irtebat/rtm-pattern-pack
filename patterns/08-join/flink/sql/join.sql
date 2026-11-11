-- Pattern 08-join: inner stream-stream join (orders and payments) on Flink SQL.
--
-- Sources: <input-base>.orders and <input-base>.payments   (contract sections 1 and 9, patterns/08-join/README.md, Benchmark setup)
-- Sink:    rtmpack.08-join.out.flink                           (key order_id)
--
-- Values in the form dollar-brace name are substituted before execution from
-- Managed Flink runtime properties (groups kafka and app), CLI arguments or
-- environment variables. See JoinSql and JobConfig.
--
-- Statements are separated by semicolons. Whole-line comments are removed
-- before the script is split.

SET 'pipeline.name' = 'rtmpack-flink-08-join';
SET 'table.local-time-zone' = 'UTC';
-- An interval join removes its state by watermark, so the state TTL does not govern it.
-- The setting is kept so every pattern in the pack resolves the same parameters.
SET 'table.exec.state.ttl' = '${app.state.ttl.ms} ms';

CREATE TABLE join_orders (
  order_id   STRING,
  `key`      STRING,
  event_ts   BIGINT,
  amount     BIGINT,
  payload    STRING,
  in_ts_ltz  TIMESTAMP_LTZ(3) METADATA FROM 'timestamp',
  event_time AS TO_TIMESTAMP_LTZ(event_ts, 3),
  WATERMARK FOR event_time AS event_time - INTERVAL '5' SECOND
) WITH (
  'connector' = 'kafka',
  'topic' = '${app.input.topic}.orders',
  'properties.bootstrap.servers' = '${kafka.bootstrap.servers}',
  'properties.security.protocol' = 'SASL_SSL',
  'properties.sasl.mechanism' = 'SCRAM-SHA-512',
  'properties.sasl.jaas.config' = '${kafka.jaas.config}',
  'scan.startup.mode' = 'latest-offset',
  'format' = 'json'
);

CREATE TABLE join_payments (
  payment_id STRING,
  order_id   STRING,
  event_ts   BIGINT,
  `method`   STRING,
  payload    STRING,
  in_ts_ltz  TIMESTAMP_LTZ(3) METADATA FROM 'timestamp',
  event_time AS TO_TIMESTAMP_LTZ(event_ts, 3),
  WATERMARK FOR event_time AS event_time - INTERVAL '5' SECOND
) WITH (
  'connector' = 'kafka',
  'topic' = '${app.input.topic}.payments',
  'properties.bootstrap.servers' = '${kafka.bootstrap.servers}',
  'properties.security.protocol' = 'SASL_SSL',
  'properties.sasl.mechanism' = 'SCRAM-SHA-512',
  'properties.sasl.jaas.config' = '${kafka.jaas.config}',
  'scan.startup.mode' = 'latest-offset',
  'format' = 'json'
);

CREATE TABLE join_output (
  order_id   STRING,
  payment_id STRING,
  `key`      STRING,
  amount     BIGINT,
  `method`   STRING,
  order_ts   BIGINT,
  payment_ts BIGINT,
  in_ts      BIGINT,
  engine     STRING
) WITH (
  'connector' = 'kafka',
  'topic' = '${app.output.topic}',
  'properties.bootstrap.servers' = '${kafka.bootstrap.servers}',
  'properties.security.protocol' = 'SASL_SSL',
  'properties.sasl.mechanism' = 'SCRAM-SHA-512',
  'properties.sasl.jaas.config' = '${kafka.jaas.config}',
  'key.format' = 'raw',
  'key.fields' = 'order_id',
  'format' = 'json',
  'sink.delivery-guarantee' = 'at-least-once',
  'properties.linger.ms' = '0'
);

-- Interval join over the rowtime attributes: a payment matches its order when
-- its event time is between the order's event time and 30 seconds later, both
-- ends included. State on both sides is cleaned up by the watermark.
--
-- in_ts is the broker LogAppendTime of each input record, converted to epoch
-- milliseconds in UTC (the session time zone is pinned above). The output
-- carries the larger of the two, so latency is measured from the newer input.
INSERT INTO join_output
SELECT
  o.order_id,
  p.payment_id,
  o.`key`,
  o.amount,
  p.`method`,
  o.event_ts AS order_ts,
  p.event_ts AS payment_ts,
  GREATEST(
    UNIX_TIMESTAMP(CAST(o.in_ts_ltz AS STRING), 'yyyy-MM-dd HH:mm:ss.SSS') * 1000
      + EXTRACT(MILLISECOND FROM o.in_ts_ltz),
    UNIX_TIMESTAMP(CAST(p.in_ts_ltz AS STRING), 'yyyy-MM-dd HH:mm:ss.SSS') * 1000
      + EXTRACT(MILLISECOND FROM p.in_ts_ltz)
  ) AS in_ts,
  'flink' AS engine
FROM join_orders AS o
JOIN join_payments AS p
  ON o.order_id = p.order_id
 AND p.event_time BETWEEN o.event_time AND o.event_time + INTERVAL '30' SECOND;
