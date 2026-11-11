-- Pattern 03-udf: scalar UDF scoring on Flink SQL.
--
-- Source:  rtmpack.03-udf.input            (contract section 2, no duplicates)
-- Sink:    rtmpack.03-udf.out.flink        (key event_id)
--
-- score_event is the Java ScalarFunction ScoreUdf. The job registers it with
-- createTemporarySystemFunction before this script runs, and it returns one row
-- (score INT, decision STRING) per call.
--
-- Values in the form dollar-brace name are substituted before execution from
-- Managed Flink runtime properties (groups kafka and app), CLI arguments or
-- environment variables. See UdfSql and JobConfig.
--
-- Statements are separated by semicolons. Whole-line comments are removed
-- before the script is split.

SET 'pipeline.name' = 'rtmpack-flink-03-udf';
SET 'table.local-time-zone' = 'UTC';

CREATE TABLE udf_input (
  event_id  STRING,
  `key`     STRING,
  seq       BIGINT,
  event_ts  BIGINT,
  is_dup    BOOLEAN,
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
-- JSON value by value.fields-include = EXCEPT_KEY, so event_id is a separate
-- column that stays in the value.
CREATE TABLE udf_output (
  kafka_key STRING,
  event_id  STRING,
  `key`     STRING,
  seq       BIGINT,
  event_ts  BIGINT,
  score     INT,
  decision  STRING,
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

-- One call to score_event per record; the inner query produces the (score,
-- decision) row and the outer query reads its fields.
--
-- in_ts is the broker timestamp of the input record, converted to epoch
-- milliseconds in UTC (the session time zone is pinned above). payload is read
-- but not written, as the contract output has no payload field.
INSERT INTO udf_output
SELECT
  e.event_id AS kafka_key,
  e.event_id,
  e.`key`,
  e.seq,
  e.event_ts,
  e.r.score AS score,
  e.r.decision AS decision,
  UNIX_TIMESTAMP(CAST(e.in_ts_ltz AS STRING), 'yyyy-MM-dd HH:mm:ss.SSS') * 1000
    + EXTRACT(MILLISECOND FROM e.in_ts_ltz) AS in_ts,
  'flink' AS engine
FROM (
  SELECT event_id, `key`, seq, event_ts, in_ts_ltz, score_event(event_id, `key`) AS r
  FROM udf_input
) AS e;
