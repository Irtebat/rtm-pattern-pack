-- Pattern 04-routing: demux and fan-out on Flink SQL.
--
-- Source:  rtmpack.04-routing.input                 (patterns/04-routing/README.md, Benchmark setup)
-- Sinks:   <base>.payment, <base>.order, <base>.click, <base>.login   (demux)
--          <base>.all                                                 (fan-out)
--          where <base> is app.output.topic, default rtmpack.04-routing.out.flink.
--          Kafka key is event_id on every sink.
--
-- Values in the form dollar-brace name are substituted before execution from
-- Managed Flink runtime properties (groups kafka and app), CLI arguments or
-- environment variables. See RoutingSql and JobConfig.
--
-- Statements are separated by semicolons. Whole-line comments are removed
-- before the script is split. The job runs the five INSERT statements as one
-- statement set (StatementSet.addInsertSql), so there is one job, one source
-- scan and five sinks. The same five INSERT statements are the statement set
-- written as EXECUTE STATEMENT SET BEGIN ... END in the Flink SQL client.

SET 'pipeline.name' = 'rtmpack-flink-04-routing';
SET 'table.local-time-zone' = 'UTC';

CREATE TABLE routing_input (
  event_id  STRING,
  `key`     STRING,
  seq       BIGINT,
  event_ts  BIGINT,
  is_dup    BOOLEAN,
  `type`    STRING,
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

-- Sink tables. kafka_key is the record key. It is listed in key.fields and
-- removed from the JSON value by value.fields-include = EXCEPT_KEY. The value
-- carries the input fields without payload, plus in_ts and engine. The five
-- tables differ only in topic.

CREATE TABLE routing_out_payment (
  kafka_key STRING,
  event_id  STRING,
  `key`     STRING,
  seq       BIGINT,
  event_ts  BIGINT,
  is_dup    BOOLEAN,
  `type`    STRING,
  in_ts     BIGINT,
  engine    STRING
) WITH (
  'connector' = 'kafka',
  'topic' = '${app.output.topic}.payment',
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

CREATE TABLE routing_out_order (
  kafka_key STRING,
  event_id  STRING,
  `key`     STRING,
  seq       BIGINT,
  event_ts  BIGINT,
  is_dup    BOOLEAN,
  `type`    STRING,
  in_ts     BIGINT,
  engine    STRING
) WITH (
  'connector' = 'kafka',
  'topic' = '${app.output.topic}.order',
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

CREATE TABLE routing_out_click (
  kafka_key STRING,
  event_id  STRING,
  `key`     STRING,
  seq       BIGINT,
  event_ts  BIGINT,
  is_dup    BOOLEAN,
  `type`    STRING,
  in_ts     BIGINT,
  engine    STRING
) WITH (
  'connector' = 'kafka',
  'topic' = '${app.output.topic}.click',
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

CREATE TABLE routing_out_login (
  kafka_key STRING,
  event_id  STRING,
  `key`     STRING,
  seq       BIGINT,
  event_ts  BIGINT,
  is_dup    BOOLEAN,
  `type`    STRING,
  in_ts     BIGINT,
  engine    STRING
) WITH (
  'connector' = 'kafka',
  'topic' = '${app.output.topic}.login',
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

CREATE TABLE routing_out_all (
  kafka_key STRING,
  event_id  STRING,
  `key`     STRING,
  seq       BIGINT,
  event_ts  BIGINT,
  is_dup    BOOLEAN,
  `type`    STRING,
  in_ts     BIGINT,
  engine    STRING
) WITH (
  'connector' = 'kafka',
  'topic' = '${app.output.topic}.all',
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

-- in_ts is the broker timestamp of the input record, converted to epoch
-- milliseconds in UTC (the session time zone is pinned above). payload is read
-- but not written.

INSERT INTO routing_out_payment
SELECT
  event_id AS kafka_key,
  event_id,
  `key`,
  seq,
  event_ts,
  is_dup,
  `type`,
  UNIX_TIMESTAMP(CAST(in_ts_ltz AS STRING), 'yyyy-MM-dd HH:mm:ss.SSS') * 1000
    + EXTRACT(MILLISECOND FROM in_ts_ltz) AS in_ts,
  'flink' AS engine
FROM routing_input
WHERE `type` = 'payment';

INSERT INTO routing_out_order
SELECT
  event_id AS kafka_key,
  event_id,
  `key`,
  seq,
  event_ts,
  is_dup,
  `type`,
  UNIX_TIMESTAMP(CAST(in_ts_ltz AS STRING), 'yyyy-MM-dd HH:mm:ss.SSS') * 1000
    + EXTRACT(MILLISECOND FROM in_ts_ltz) AS in_ts,
  'flink' AS engine
FROM routing_input
WHERE `type` = 'order';

INSERT INTO routing_out_click
SELECT
  event_id AS kafka_key,
  event_id,
  `key`,
  seq,
  event_ts,
  is_dup,
  `type`,
  UNIX_TIMESTAMP(CAST(in_ts_ltz AS STRING), 'yyyy-MM-dd HH:mm:ss.SSS') * 1000
    + EXTRACT(MILLISECOND FROM in_ts_ltz) AS in_ts,
  'flink' AS engine
FROM routing_input
WHERE `type` = 'click';

INSERT INTO routing_out_login
SELECT
  event_id AS kafka_key,
  event_id,
  `key`,
  seq,
  event_ts,
  is_dup,
  `type`,
  UNIX_TIMESTAMP(CAST(in_ts_ltz AS STRING), 'yyyy-MM-dd HH:mm:ss.SSS') * 1000
    + EXTRACT(MILLISECOND FROM in_ts_ltz) AS in_ts,
  'flink' AS engine
FROM routing_input
WHERE `type` = 'login';

INSERT INTO routing_out_all
SELECT
  event_id AS kafka_key,
  event_id,
  `key`,
  seq,
  event_ts,
  is_dup,
  `type`,
  UNIX_TIMESTAMP(CAST(in_ts_ltz AS STRING), 'yyyy-MM-dd HH:mm:ss.SSS') * 1000
    + EXTRACT(MILLISECOND FROM in_ts_ltz) AS in_ts,
  'flink' AS engine
FROM routing_input;
