-- Pattern 06-tumbling: tumbling 60 s count and sum per key, on Flink SQL.
--
-- Source:  rtmpack.06-tumbling.input            (contract sections 1 and 2, patterns/06-tumbling/README.md, Benchmark setup)
-- Sink:    rtmpack.06-tumbling.out.flink        (patterns/06-tumbling/README.md, Benchmark setup, key "<key>:<window_start_ms>")
--
-- Values in the form dollar-brace name are substituted before execution from
-- Managed Flink runtime properties (groups kafka and app), CLI arguments or
-- environment variables. See TumblingSql and JobConfig.
--
-- Statements are separated by semicolons. Whole-line comments are removed
-- before the script is split.

SET 'pipeline.name' = 'rtmpack-flink-06-tumbling';
SET 'table.local-time-zone' = 'UTC';
SET 'table.exec.state.ttl' = '${app.state.ttl.ms} ms';

CREATE TABLE tumbling_input (
  event_id   STRING,
  `key`      STRING,
  seq        BIGINT,
  event_ts   BIGINT,
  amount     BIGINT,
  payload    STRING,
  in_ts_ltz  TIMESTAMP_LTZ(3) METADATA FROM 'timestamp'
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

-- upsert-kafka sink. The record key is kafka_key (raw string, the primary key);
-- the value is JSON without kafka_key. Updates are written as they arrive.
-- Changelog: the aggregate is grouped on kafka_key, which is the sink's primary
-- key, so the planner infers an upsert key equal to the primary key. It then
-- adds no SinkUpsertMaterializer and the aggregate emits only +I and +U (no -U).
-- This query never produces DELETE, so no tombstones are written.
CREATE TABLE tumbling_output (
  kafka_key        STRING,
  `key`            STRING,
  window_start_ms  BIGINT,
  window_end_ms    BIGINT,
  `count`          BIGINT,
  sum_amount       BIGINT,
  in_ts            BIGINT,
  engine           STRING,
  PRIMARY KEY (kafka_key) NOT ENFORCED
) WITH (
  'connector' = 'upsert-kafka',
  'topic' = '${app.output.topic}',
  'properties.bootstrap.servers' = '${kafka.bootstrap.servers}',
  'properties.security.protocol' = 'SASL_SSL',
  'properties.sasl.mechanism' = 'SCRAM-SHA-512',
  'properties.sasl.jaas.config' = '${kafka.jaas.config}',
  'key.format' = 'raw',
  'value.format' = 'json',
  'value.fields-include' = 'EXCEPT_KEY',
  'properties.linger.ms' = '0'
);

-- Regular aggregation, not the window TVF: the TVF emits only when the window
-- closes, and this pattern needs the running count after every input.
-- window_start is the event time rounded down to a multiple of 60000 ms (epoch
-- aligned, the same windows as Spark's window(event_time, "60 seconds")).
--
-- The group is (key, window_start). It is written as GROUP BY kafka_key, where
-- kafka_key = key || ':' || window_start, and key and window_start are carried
-- with MAX. The two forms define the same groups. Grouping on kafka_key makes the
-- aggregate's upsert key equal to the sink's primary key.
--
-- in_ts is the broker LogAppendTime of the input record in epoch milliseconds
-- (the session time zone is pinned above). The output carries the largest
-- in_ts seen in the window so far. State for a group is dropped after
-- table.exec.state.ttl without a new input.
INSERT INTO tumbling_output
SELECT
  kafka_key,
  MAX(`key`) AS `key`,
  MAX(window_start) AS window_start_ms,
  MAX(window_start) + 60000 AS window_end_ms,
  COUNT(*) AS `count`,
  SUM(amount) AS sum_amount,
  MAX(in_ts) AS in_ts,
  'flink' AS engine
FROM (
  SELECT
    CONCAT(`key`, ':', CAST(event_ts - MOD(event_ts, 60000) AS STRING)) AS kafka_key,
    `key`,
    amount,
    event_ts - MOD(event_ts, 60000) AS window_start,
    UNIX_TIMESTAMP(CAST(in_ts_ltz AS STRING), 'yyyy-MM-dd HH:mm:ss.SSS') * 1000
      + EXTRACT(MILLISECOND FROM in_ts_ltz) AS in_ts
  FROM tumbling_input
  WHERE `key` IS NOT NULL AND event_ts IS NOT NULL
)
GROUP BY kafka_key;
