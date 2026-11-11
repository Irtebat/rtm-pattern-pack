package com.rtmpack.tumbling;

import org.apache.flink.api.common.typeinfo.Types;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.DataTypes;
import org.apache.flink.table.api.EnvironmentSettings;
import org.apache.flink.table.api.ExplainDetail;
import org.apache.flink.table.api.Schema;
import org.apache.flink.table.api.Table;
import org.apache.flink.table.api.TableEnvironment;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;
import org.apache.flink.types.Row;
import org.apache.flink.types.RowKind;
import org.apache.flink.util.CloseableIterator;
import org.junit.jupiter.api.Test;

import java.time.Instant;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.regex.Pattern;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * Tests for 06-tumbling (tumbling window) using the real statements from tumbling.sql.
 *
 * <p>Plan tests execute the full script's DDL and explain the INSERT, so the Kafka and
 * upsert-kafka tables are validated by the planner without a broker. Result tests replace the
 * Kafka source with a bounded in-memory table of the same columns and run the INSERT query as a
 * SELECT. Everything else is the production SQL.
 */
class TumblingSqlTest {

    /** Width of the window in milliseconds. */
    private static final long WINDOW_MS = 60_000L;
    /** Distance between window starts in milliseconds. */
    private static final long STEP_MS = 60000L;
    private static final Pattern KAFKA_DDL = Pattern.compile("(?is)^CREATE\\s+TABLE\\s+tumbling_(input|output)\\b.*");

    private static String script() throws Exception {
        JobConfig config = JobConfig.resolve(
                new String[] {
                    "--kafka.bootstrap.servers", "b-1.example:9096",
                    "--kafka.username", "rtmpack",
                    "--kafka.password", "it's \"secret\""
                },
                Map.of(),
                Map.of());
        String script = TumblingSql.render(TumblingSql.readResource(TumblingSql.RESOURCE), config.values());
        assertFalse(script.contains("${"), "all placeholders must be substituted");
        return script;
    }

    @Test
    void defaultsMatchContract() {
        JobConfig config = JobConfig.resolve(
                new String[] {
                    "--kafka.bootstrap.servers", "b",
                    "--kafka.username", "u",
                    "--kafka.password", "p"
                },
                Map.of(),
                Map.of());
        assertEquals(120_000L, config.stateTtlMs());
        assertEquals("rtmpack.06-tumbling.input", config.get("app.input.topic"));
        assertEquals("rtmpack.06-tumbling.out.flink", config.get("app.output.topic"));
    }

    @Test
    void scriptCompilesAndPlanHasSingleGroupAggregateAfterExchange() throws Exception {
        TableEnvironment tableEnv = TableEnvironment.create(EnvironmentSettings.inStreamingMode());
        String insert = null;
        for (String statement : TumblingSql.statements(script())) {
            if (TumblingSql.applyIfSet(tableEnv, statement)) {
                continue;
            }
            if (TumblingSql.isInsert(statement)) {
                insert = statement;
            } else {
                tableEnv.executeSql(statement);
            }
        }
        assertNotNull(insert, "tumbling.sql must contain the INSERT statement");

        String plan = tableEnv.explainSql(insert, ExplainDetail.CHANGELOG_MODE);
        String executionPlan = plan.substring(plan.indexOf("== Optimized Execution Plan =="));
        String physicalPlan = plan.substring(plan.indexOf("== Optimized Physical Plan =="),
                plan.indexOf("== Optimized Execution Plan =="));

        assertEquals(1, count(executionPlan, "GroupAggregate("), "exactly one GroupAggregate");
        assertEquals(1, count(executionPlan, "Exchange("), "exactly one exchange, in front of the aggregate");
        assertFalse(plan.contains("WindowAggregate"), "no window operator");
        assertFalse(plan.contains("GroupWindowAggregate"), "no legacy window operator");
        assertFalse(plan.contains("WindowTableFunction"), "no window TVF");
        assertFalse(plan.contains("SinkUpsertMaterializer"), "no upsert materializer in front of the sink");
        assertFalse(plan.contains("MiniBatch"), "mini-batch must stay off");
        assertFalse(plan.contains("LocalGroupAggregate"), "no local/global two-phase aggregate");

        int sink = executionPlan.indexOf("Sink(");
        int aggregate = executionPlan.indexOf("GroupAggregate(");
        int exchange = executionPlan.indexOf("Exchange(");
        int source = executionPlan.indexOf("TableSourceScan(");
        assertTrue(sink >= 0 && sink < aggregate && aggregate < exchange && exchange < source,
                "plan is source, Exchange, GroupAggregate, sink (printed sink first)");
        assertTrue(executionPlan.contains("hash[kafka_key]"), "exchange hashes on the group key");

        // Changelog: the aggregate produces updates; the upsert sink consumes only +I and +U.
        String aggregateLine = physicalPlan.lines().filter(l -> l.contains("GroupAggregate(")).findFirst().orElseThrow();
        assertTrue(aggregateLine.contains("changelogMode=[I,UA]"),
                "upsert sink needs no UPDATE_BEFORE, so the aggregate emits I and UA only: " + aggregateLine);
        assertFalse(aggregateLine.contains("UB"), "no UPDATE_BEFORE records");
    }

    @Test
    void emitsUpdatePerInputAndFinalValuesAreCorrect() throws Exception {
        String script = script();

        // Out-of-order and boundary event times, several keys and windows.
        long[] eventTs = {59_999L, 60_000L, 30_000L, 119_999L, 10_000L, 61_000L, 20_000L, 40_000L, 125_000L};
        String[] keys = {"k1", "k1", "k1", "k1", "k2", "k2", "k1", "k3", "k3"};
        long[] amounts = {100L, 250L, 40L, 7L, 500L, 12L, 3L, 9L, 11L};
        Instant base = Instant.parse("2026-11-08T10:00:00.000Z");
        List<Row> rows = new ArrayList<>();
        for (int i = 0; i < eventTs.length; i++) {
            // Arrival times are not monotonic in event time; i-th row arrives i * 137 ms after base.
            rows.add(Row.of("e" + i, keys[i], (long) i, eventTs[i], amounts[i], "pad", base.plusMillis(i * 137L)));
        }

        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);
        StreamTableEnvironment tableEnv = StreamTableEnvironment.create(env);

        DataStream<Row> stream = env.fromData(rows, Types.ROW_NAMED(
                new String[] {"event_id", "key", "seq", "event_ts", "amount", "payload", "in_ts_ltz"},
                Types.STRING, Types.STRING, Types.LONG, Types.LONG, Types.LONG, Types.STRING, Types.INSTANT));
        Table input = tableEnv.fromDataStream(stream, Schema.newBuilder()
                .column("event_id", DataTypes.STRING())
                .column("key", DataTypes.STRING())
                .column("seq", DataTypes.BIGINT())
                .column("event_ts", DataTypes.BIGINT())
                .column("amount", DataTypes.BIGINT())
                .column("payload", DataTypes.STRING())
                .column("in_ts_ltz", DataTypes.TIMESTAMP_LTZ(3))
                .build());

        String select = null;
        for (String statement : TumblingSql.statements(script)) {
            if (KAFKA_DDL.matcher(statement).matches()) {
                continue;
            }
            if (TumblingSql.isInsert(statement)) {
                select = TumblingSql.insertSelect(statement);
            } else {
                assertTrue(TumblingSql.applyIfSet(tableEnv, statement), "unexpected statement: " + statement);
            }
        }
        assertNotNull(select);
        tableEnv.createTemporaryView("tumbling_input", input);

        List<Row> changes = new ArrayList<>();
        try (CloseableIterator<Row> it = tableEnv.sqlQuery(select).execute().collect()) {
            it.forEachRemaining(changes::add);
        }

        // Expected values from a brute-force oracle: every window start s (a multiple of STEP_MS)
        // with s <= ts < s + WINDOW_MS.
        Map<String, long[]> expected = new HashMap<>(); // kafka_key -> {count, sum, max in_ts}
        int windowsPerEvent = 0;
        for (int i = 0; i < eventTs.length; i++) {
            long lastStart = eventTs[i] - Math.floorMod(eventTs[i], STEP_MS);
            int memberships = 0;
            for (long s = lastStart; s > eventTs[i] - WINDOW_MS; s -= STEP_MS) {
                long[] agg = expected.computeIfAbsent(keys[i] + ":" + s, k -> new long[] {0, 0, 0});
                agg[0]++;
                agg[1] += amounts[i];
                agg[2] = Math.max(agg[2], base.plusMillis(i * 137L).toEpochMilli());
                memberships++;
            }
            windowsPerEvent = memberships;
        }
        assertEquals(WINDOW_MS / STEP_MS, windowsPerEvent, "each event lands in WINDOW/STEP windows");

        // One insert or update-after per (input, window) pair: updates are not held back.
        long upserts = changes.stream()
                .filter(r -> r.getKind() == RowKind.INSERT || r.getKind() == RowKind.UPDATE_AFTER)
                .count();
        assertEquals((long) eventTs.length * (WINDOW_MS / STEP_MS), upserts,
                "one update per input and window, no batching");

        // Fold the changelog into the final table by kafka_key.
        Map<String, Row> last = new HashMap<>();
        for (Row r : changes) {
            String id = (String) r.getField(0);
            if (r.getKind() == RowKind.INSERT || r.getKind() == RowKind.UPDATE_AFTER) {
                last.put(id, r);
            } else if (r.getKind() == RowKind.DELETE) {
                last.remove(id);
            }
        }

        assertEquals(expected.keySet(), last.keySet(), "same set of (key, window) outputs");
        for (Map.Entry<String, long[]> e : expected.entrySet()) {
            Row r = last.get(e.getKey());
            String[] parts = e.getKey().split(":");
            long start = Long.parseLong(parts[1]);
            assertEquals(parts[0], r.getField(1), "key");
            assertEquals(start, r.getField(2), "window_start_ms");
            assertEquals(start + WINDOW_MS, r.getField(3), "window_end_ms");
            assertEquals(e.getValue()[0], r.getField(4), "count for " + e.getKey());
            assertEquals(e.getValue()[1], r.getField(5), "sum_amount for " + e.getKey());
            assertEquals(e.getValue()[2], r.getField(6), "in_ts is the max in_ts of the window for " + e.getKey());
            assertEquals("flink", r.getField(7));
        }

        // Counts grow monotonically per output key (running count).
        Map<String, Long> seen = new HashMap<>();
        for (Row r : changes) {
            if (r.getKind() == RowKind.INSERT || r.getKind() == RowKind.UPDATE_AFTER) {
                long c = (Long) r.getField(4);
                Long prev = seen.put((String) r.getField(0), c);
                assertTrue(prev == null || c > prev, "count must increase with every update");
            }
        }
    }

    private static int count(String text, String needle) {
        int n = 0;
        for (int i = text.indexOf(needle); i >= 0; i = text.indexOf(needle, i + 1)) {
            n++;
        }
        return n;
    }
}
