package com.rtmpack.dedup;

import org.apache.flink.api.common.typeinfo.Types;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.DataTypes;
import org.apache.flink.table.api.Schema;
import org.apache.flink.table.api.Table;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;
import org.apache.flink.types.Row;
import org.apache.flink.util.CloseableIterator;
import org.junit.jupiter.api.Test;

import java.time.Instant;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.regex.Pattern;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * Runs the real dedup statements from dedup.sql on a bounded in-memory source.
 *
 * <p>The two Kafka DDL statements are not executed. The source table is replaced by a view that
 * has the same columns and a processing-time attribute, and the INSERT query is run as a SELECT
 * with its rows collected. Everything else (session settings, ROW_NUMBER dedup, the in_ts
 * conversion) is the production SQL.
 */
class DedupSqlTest {

    private static final Pattern KAFKA_DDL = Pattern.compile("(?is)^CREATE\\s+TABLE\\s+dedup_(input|output)\\b.*");

    @Test
    void keepsOneRowPerEventIdAndConvertsInTsToEpochMillis() throws Exception {
        JobConfig config = JobConfig.resolve(
                new String[] {
                    "--kafka.bootstrap.servers", "b-1.example:9096",
                    "--kafka.username", "rtmpack",
                    "--kafka.password", "it's \"secret\""
                },
                Map.of(),
                Map.of());

        String script = DedupSql.render(DedupSql.readResource(DedupSql.RESOURCE), config.values());
        assertTrue(!script.contains("${"), "all placeholders must be substituted");

        Instant e2Ts = Instant.parse("2026-11-08T10:15:31.456Z");
        Instant e1First = Instant.parse("2026-11-08T10:15:30.123Z");
        List<Row> rows = new ArrayList<>();
        rows.add(row("e1", "user-1", 1L, 1000L, false, "pad", e1First));
        rows.add(row("e1", "user-1", 1L, 1000L, true, "pad", e1First.plusMillis(5)));
        rows.add(row("e2", "user-2", 2L, 2000L, false, "pad", e2Ts));
        rows.add(row("e3", "user-3", 3L, 3000L, false, "pad", e2Ts.plusSeconds(1)));
        rows.add(row("e3", "user-3", 3L, 3000L, true, "pad", e2Ts.plusSeconds(2)));

        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);
        StreamTableEnvironment tableEnv = StreamTableEnvironment.create(env);

        DataStream<Row> stream = env.fromData(rows, Types.ROW_NAMED(
                new String[] {"event_id", "key", "seq", "event_ts", "is_dup", "payload", "in_ts_ltz"},
                Types.STRING, Types.STRING, Types.LONG, Types.LONG, Types.BOOLEAN, Types.STRING, Types.INSTANT));
        Table input = tableEnv.fromDataStream(stream, Schema.newBuilder()
                .column("event_id", DataTypes.STRING())
                .column("key", DataTypes.STRING())
                .column("seq", DataTypes.BIGINT())
                .column("event_ts", DataTypes.BIGINT())
                .column("is_dup", DataTypes.BOOLEAN())
                .column("payload", DataTypes.STRING())
                .column("in_ts_ltz", DataTypes.TIMESTAMP_LTZ(3))
                .columnByExpression("proc_time", "PROCTIME()")
                .build());

        String select = null;
        for (String statement : DedupSql.statements(script)) {
            if (KAFKA_DDL.matcher(statement).matches()) {
                continue;
            }
            if (DedupSql.isInsert(statement)) {
                select = DedupSql.insertSelect(statement);
            } else {
                // SET statements: pipeline name, time zone and state TTL.
                assertTrue(DedupSql.applyIfSet(tableEnv, statement), "unexpected statement: " + statement);
            }
        }
        assertTrue(select != null, "dedup.sql must contain the INSERT statement");
        tableEnv.createTemporaryView("dedup_input", input);

        List<Row> out = new ArrayList<>();
        try (CloseableIterator<Row> it = tableEnv.sqlQuery(select).execute().collect()) {
            it.forEachRemaining(out::add);
        }

        assertEquals(3, out.size(), "one output row per distinct event_id");
        Set<String> ids = new HashSet<>();
        Map<String, Row> byId = new HashMap<>();
        for (Row r : out) {
            String id = (String) r.getField(0);
            ids.add(id);
            byId.put(id, r);
            assertEquals("flink", r.getField(7));
        }
        assertEquals(Set.of("e1", "e2", "e3"), ids);

        long e2In = (Long) byId.get("e2").getField(6);
        assertEquals(e2Ts.toEpochMilli(), e2In, "in_ts must be epoch millis of the input timestamp");
        long e1In = (Long) byId.get("e1").getField(6);
        assertTrue(e1In == e1First.toEpochMilli() || e1In == e1First.plusMillis(5).toEpochMilli(),
                "e1 must be one of its two arrivals");
        long e3In = (Long) byId.get("e3").getField(6);
        assertTrue(e3In == e2Ts.plusSeconds(1).toEpochMilli() || e3In == e2Ts.plusSeconds(2).toEpochMilli(),
                "e3 must be one of its two arrivals");
    }

    private static Row row(String eventId, String key, long seq, long eventTs, boolean isDup,
                           String payload, Instant inTs) {
        return Row.of(eventId, key, seq, eventTs, isDup, payload, inTs);
    }
}
