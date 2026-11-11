-- Pattern 05-lookup: lookup join with a small reference table on Flink SQL.
--
-- Source:  rtmpack.05-lookup.input            (contract section 2, patterns/05-lookup/README.md, Benchmark setup)
-- Sink:    rtmpack.05-lookup.out.flink        (key event_id)
--
-- Values in the form dollar-brace name are substituted before execution from
-- Managed Flink runtime properties (groups kafka and app), CLI arguments or
-- environment variables. See LookupSql and JobConfig.
--
-- Statements are separated by semicolons. Whole-line comments are removed
-- before the script is split.

SET 'pipeline.name' = 'rtmpack-flink-05-lookup';
SET 'table.local-time-zone' = 'UTC';

-- The function loads /merchants.csv from the jar into a HashMap in open().
CREATE TEMPORARY SYSTEM FUNCTION lookup_merchant AS 'com.rtmpack.lookup.MerchantLookup' LANGUAGE JAVA;

CREATE TABLE lookup_input (
  event_id    STRING,
  `key`       STRING,
  seq         BIGINT,
  event_ts    BIGINT,
  is_dup      BOOLEAN,
  merchant_id STRING,
  payload     STRING,
  in_ts_ltz   TIMESTAMP_LTZ(3) METADATA FROM 'timestamp'
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

-- kafka_key is the record key (event_id). It is listed in key.fields and removed
-- from the JSON value by value.fields-include = EXCEPT_KEY.
CREATE TABLE lookup_output (
  kafka_key   STRING,
  event_id    STRING,
  `key`       STRING,
  event_ts    BIGINT,
  merchant_id STRING,
  risk_tier   STRING,
  category    STRING,
  country     STRING,
  in_ts       BIGINT,
  engine      STRING
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

-- CROSS JOIN LATERAL gives inner semantics: an event whose merchant_id is not in
-- the table produces no row.
--
-- in_ts is the broker timestamp of the input record, converted to epoch
-- milliseconds in UTC (the session time zone is pinned above).
INSERT INTO lookup_output
SELECT
  e.event_id AS kafka_key,
  e.event_id,
  e.`key`,
  e.event_ts,
  e.merchant_id,
  m.risk_tier,
  m.category,
  m.country,
  UNIX_TIMESTAMP(CAST(e.in_ts_ltz AS STRING), 'yyyy-MM-dd HH:mm:ss.SSS') * 1000
    + EXTRACT(MILLISECOND FROM e.in_ts_ltz) AS in_ts,
  'flink' AS engine
FROM lookup_input AS e
CROSS JOIN LATERAL TABLE(lookup_merchant(e.merchant_id)) AS m(risk_tier, category, country);
