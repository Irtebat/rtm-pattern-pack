package com.rtmpack.routing;

import org.apache.flink.api.common.typeinfo.Types;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.DataTypes;
import org.apache.flink.table.api.Schema;
import org.apache.flink.table.api.StatementSet;
import org.apache.flink.table.api.Table;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;
import org.apache.flink.types.Row;
import org.apache.flink.util.CloseableIterator;
import org.junit.jupiter.api.Test;

import java.time.Instant;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * Runs the real statements from routing.sql on a bounded in-memory source.
 *
 * <p>The source Kafka DDL is replaced by a view with the same columns. The five sink DDL
 * statements are executed (they need no broker), the five INSERT statements are added to one
 * statement set and planned against the real sink options, and each INSERT query is run as a
 * SELECT with its rows collected.
 */
class RoutingSqlTest {

    private static final Pattern SOURCE_DDL = Pattern.compile("(?is)^CREATE\\s+TABLE\\s+routing_input\\b.*");
    private static final Pattern INSERT_TARGET = Pattern.compile("(?is)^\\s*INSERT\\s+INTO\\s+(\\S+)\\s.*");
    private static final List<String> TYPES = List.of("payment", "order", "click", "login");

    private static String script() throws Exception {
        JobConfig config = JobConfig.resolve(
                new String[] {
                    "--kafka.bootstrap.servers", "b-1.example:9096",
                    "--kafka.username", "rtmpack",
                    "--kafka.password", "it's \"secret\"",
                    "--app.state.ttl.ms", "60000",
                    "--app.output.topic", "rtmpack.04-routing.out.flink"
                },
                Map.of(),
                Map.of());
        String script = RoutingSql.render(RoutingSql.readResource(RoutingSql.RESOURCE), config.values());
        assertTrue(!script.contains("${"), "all placeholders must be substituted");
        return script;
    }

    private static Row event(String id, String type, Instant ts) {
        return Row.of(id, "user-00042", 5L, 1000L, false, type, "pad", ts);
    }

    @Test
    void routesEachEventToItsTypeTopicAndToAll() throws Exception {
        String script = script();

        Instant ts = Instant.parse("2026-11-08T10:15:30.123Z");
        List<Row> rows = new ArrayList<>();
        // 4 payment, 3 order, 2 click, 1 login
        int n = 0;
        for (int i = 0; i < 4; i++) rows.add(event("e-" + n++, "payment", ts));
        for (int i = 0; i < 3; i++) rows.add(event("e-" + n++, "order", ts));
        for (int i = 0; i < 2; i++) rows.add(event("e-" + n++, "click", ts));
        rows.add(event("e-" + n++, "login", ts));

        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);
        StreamTableEnvironment tableEnv = StreamTableEnvironment.create(env);

        DataStream<Row> stream = env.fromData(rows, Types.ROW_NAMED(
                new String[] {"event_id", "key", "seq", "event_ts", "is_dup", "type", "payload", "in_ts_ltz"},
                Types.STRING, Types.STRING, Types.LONG, Types.LONG, Types.BOOLEAN, Types.STRING, Types.STRING,
                Types.INSTANT));
        Table input = tableEnv.fromDataStream(stream, Schema.newBuilder()
                .column("event_id", DataTypes.STRING())
                .column("key", DataTypes.STRING())
                .column("seq", DataTypes.BIGINT())
                .column("event_ts", DataTypes.BIGINT())
                .column("is_dup", DataTypes.BOOLEAN())
                .column("type", DataTypes.STRING())
                .column("payload", DataTypes.STRING())
                .column("in_ts_ltz", DataTypes.TIMESTAMP_LTZ(3))
                .build());
        tableEnv.createTemporaryView("routing_input", input);

        StatementSet statementSet = tableEnv.createStatementSet();
        Map<String, String> selectByTable = new java.util.LinkedHashMap<>();
        int sinkTables = 0;
        for (String statement : RoutingSql.statements(script)) {
            if (SOURCE_DDL.matcher(statement).matches()) {
                continue;
            }
            if (RoutingSql.isInsert(statement)) {
                statementSet.addInsertSql(statement);
                Matcher m = INSERT_TARGET.matcher(statement);
                assertTrue(m.matches());
                selectByTable.put(m.group(1), RoutingSql.insertSelect(statement));
            } else if (!RoutingSql.applyIfSet(tableEnv, statement)) {
                tableEnv.executeSql(statement); // sink DDL
                sinkTables++;
            }
        }
        assertEquals(5, sinkTables, "five sink tables");
        assertEquals(5, selectByTable.size(), "five INSERT statements in one statement set");

        // The statement set plans against the real Kafka sink options. Nothing in it needs a
        // shuffle: each INSERT is a filter and a projection over the source.
        String plan = statementSet.explain();
        assertFalse(plan.contains("Exchange"), "demux must not add an exchange:\n" + plan);
        for (String table : selectByTable.keySet()) {
            assertTrue(plan.contains("Sink(table=[default_catalog.default_database." + table + "]"),
                    "plan must write to " + table + ":\n" + plan);
        }
        // All five sinks hang off a single source scan in the executed plan.
        String executionPlan = plan.substring(plan.indexOf("== Optimized Execution Plan =="));
        assertEquals(1, count(executionPlan, "TableSourceScan("), "one source scan:\n" + executionPlan);

        // Collect each INSERT's rows.
        // Columns: kafka_key, event_id, key, seq, event_ts, is_dup, type, in_ts, engine
        int total = 0;
        for (Map.Entry<String, String> entry : selectByTable.entrySet()) {
            String table = entry.getKey();
            List<Row> out = new ArrayList<>();
            try (CloseableIterator<Row> it = tableEnv.sqlQuery(entry.getValue()).execute().collect()) {
                it.forEachRemaining(out::add);
            }
            total += out.size();
            String suffix = table.substring("routing_out_".length());
            int expected = switch (suffix) {
                case "payment" -> 4;
                case "order" -> 3;
                case "click" -> 2;
                case "login" -> 1;
                case "all" -> 10;
                default -> throw new AssertionError("unexpected sink " + table);
            };
            assertEquals(expected, out.size(), table);
            for (Row r : out) {
                assertEquals(r.getField(1), r.getField(0), "Kafka key is event_id");
                assertEquals(9, r.getArity(), "no payload column");
                if (!suffix.equals("all")) {
                    assertEquals(suffix, r.getField(6), "type must match the topic " + table);
                }
                assertEquals("flink", r.getField(8));
                assertEquals(ts.toEpochMilli(), r.getField(7), "in_ts must be epoch millis");
                assertEquals(5L, r.getField(3));
                assertEquals(1000L, r.getField(4));
                assertEquals(false, r.getField(5));
            }
        }
        assertEquals(20, total, "two outputs per event");
    }

    @Test
    void sinkTopicsAreBaseDotTypeAndBaseDotAll() throws Exception {
        String script = script();
        for (String t : TYPES) {
            assertTrue(script.contains("'topic' = 'rtmpack.04-routing.out.flink." + t + "'"), t);
        }
        assertTrue(script.contains("'topic' = 'rtmpack.04-routing.out.flink.all'"));
        assertTrue(script.contains("'topic' = 'rtmpack.04-routing.input'"));
    }

    @Test
    void sinksKeepKeyOutOfValue() throws Exception {
        String sql = RoutingSql.readResource(RoutingSql.RESOURCE);
        assertEquals(5, count(sql, "'key.fields' = 'kafka_key'"));
        assertEquals(5, count(sql, "'value.fields-include' = 'EXCEPT_KEY'"));
        assertEquals(5, count(sql, "'properties.linger.ms' = '0'"));
        assertEquals(5, count(sql, "'sink.delivery-guarantee' = 'at-least-once'"));
    }

    @Test
    void defaultTopicsFollowContract() {
        JobConfig config = JobConfig.resolve(
                new String[] {
                    "--kafka.bootstrap.servers", "b:9096", "--kafka.username", "u", "--kafka.password", "p"
                },
                Map.of(),
                Map.of());
        assertEquals("rtmpack.04-routing.input", config.get("app.input.topic"));
        assertEquals("rtmpack.04-routing.out.flink", config.get("app.output.topic"));
    }

    private static int count(String text, String needle) {
        int c = 0;
        for (int i = text.indexOf(needle); i >= 0; i = text.indexOf(needle, i + 1)) {
            c++;
        }
        return c;
    }
}
