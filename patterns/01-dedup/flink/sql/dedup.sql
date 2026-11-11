-- Pattern 01-dedup: deduplication, keep first per key, on Flink SQL.
--
-- Source:  rtmpack.01-dedup.input            (contract sections 1 and 2)
-- Sink:    rtmpack.01-dedup.out.flink        (contract section 3, key event_id)
--
-- Values in the form dollar-brace name are substituted before execution from
-- Managed Flink runtime properties (groups kafka and app), CLI arguments or
-- environment variables. See DedupSql and JobConfig.
--
-- Statements are separated by semicolons. Whole-line comments are removed
-- before the script is split.

SET 'pipeline.name' = 'rtmpack-flink-01-dedup';
SET 'table.local-time-zone' = 'UTC';
SET 'table.exec.state.ttl' = '${app.state.ttl.ms} ms';

CREATE TABLE dedup_input (
  event_id   STRING,
  `key`      STRING,
  seq        BIGINT,
  event_ts   BIGINT,
  is_dup     BOOLEAN,
  payload    STRING,
  in_ts_ltz  TIMESTAMP_LTZ(3) METADATA FROM 'timestamp',
  proc_time  AS PROCTIME()
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

CREATE TABLE dedup_output (
  event_id  STRING,
  `key`     STRING,
  seq       BIGINT,
  event_ts  BIGINT,
  is_dup    BOOLEAN,
  payload   STRING,
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
  'key.fields' = 'event_id',
  'format' = 'json',
  'sink.delivery-guarantee' = 'at-least-once',
  'properties.linger.ms' = '0'
);

-- First-row deduplication. ORDER BY on the processing-time attribute in
-- ascending order makes ROW_NUMBER keep the first arrival per event_id,
-- so the result is append-only. State for each event_id is cleaned up
-- after table.exec.state.ttl without a new arrival.
--
-- in_ts is the broker LogAppendTime of the input record, converted to epoch
-- milliseconds in UTC (the session time zone is pinned above).
INSERT INTO dedup_output
SELECT
  event_id,
  `key`,
  seq,
  event_ts,
  is_dup,
  payload,
  UNIX_TIMESTAMP(CAST(in_ts_ltz AS STRING), 'yyyy-MM-dd HH:mm:ss.SSS') * 1000
    + EXTRACT(MILLISECOND FROM in_ts_ltz) AS in_ts,
  'flink' AS engine
FROM (
  SELECT
    *,
    ROW_NUMBER() OVER (PARTITION BY event_id ORDER BY proc_time ASC) AS row_num
  FROM dedup_input
)
WHERE row_num = 1;
