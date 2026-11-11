package com.rtmpack.udf;

import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.api.common.typeinfo.Types;
import org.apache.flink.table.api.DataTypes;
import org.apache.flink.table.api.Schema;
import org.apache.flink.table.api.Table;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;
import org.apache.flink.types.Row;
import org.apache.flink.util.CloseableIterator;
import org.junit.jupiter.api.Test;

import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.time.Instant;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.regex.Pattern;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * Runs the real statements from udf.sql on a bounded in-memory source, with the Java ScoreUdf
 * registered the same way the job registers it.
 *
 * <p>The source Kafka DDL is replaced by a view with the same columns. The sink DDL is executed
 * (it needs no broker) so the final INSERT is planned against the real sink options. The INSERT
 * query is run as a SELECT with its rows collected.
 *
 * <p>The expected scores come from {@code patterns/03-udf/score_vectors.csv}, the file the Python
 * tests of the RTM side assert as well.
 */
class UdfSqlTest {

    private static final Pattern SOURCE_DDL = Pattern.compile("(?is)^CREATE\\s+TABLE\\s+udf_input\\b.*");

    static final java.util.concurrent.atomic.AtomicInteger CALLS = new java.util.concurrent.atomic.AtomicInteger();

    /** Same function with a call counter, to check that the SQL evaluates it once per record. */
    public static class CountingScoreUdf extends ScoreUdf {
        @Override
        public Row eval(String eventId, String key) {
            CALLS.incrementAndGet();
            return super.eval(eventId, key);
        }
    }

    /** One line of the shared vector file. */
    record Vector(String eventId, String key, int score, String decision) {
    }

    /** Maven runs tests with the module directory as working directory. */
    static List<Vector> vectors() throws IOException {
        Path file = Path.of("..", "score_vectors.csv");
        List<String> lines = Files.readAllLines(file, StandardCharsets.UTF_8);
        assertEquals("event_id,key,score,decision", lines.get(0));
        List<Vector> out = new ArrayList<>();
        for (String line : lines.subList(1, lines.size())) {
            if (line.isBlank()) {
                continue;
            }
            String[] f = line.split(",", -1);
            out.add(new Vector(f[0], f[1], Integer.parseInt(f[2]), f[3]));
        }
        return out;
    }

    @Test
    void sharedVectorMatchesFunction() throws Exception {
        List<Vector> vectors = vectors();
        assertEquals(5, vectors.size());
        ScoreUdf udf = new ScoreUdf();
        for (Vector v : vectors) {
            Row r = udf.eval(v.eventId(), v.key());
            assertEquals(v.score(), r.getField(0), v.eventId());
            assertEquals(v.decision(), r.getField(1), v.eventId());
            assertEquals(v.score(), ScoreUdf.score(v.eventId(), v.key()));
        }
        assertEquals(3, vectors.stream().map(Vector::decision).distinct().count(), "vector must cover all decisions");
    }

    @Test
    void decisionThresholds() {
        assertEquals("approve", ScoreUdf.decision(0));
        assertEquals("approve", ScoreUdf.decision(799));
        assertEquals("review", ScoreUdf.decision(800));
        assertEquals("review", ScoreUdf.decision(949));
        assertEquals("flag", ScoreUdf.decision(950));
        assertEquals("flag", ScoreUdf.decision(999));
    }

    @Test
    void nullArgumentsGiveNull() {
        ScoreUdf udf = new ScoreUdf();
        assertNull(udf.eval(null, "k"));
        assertNull(udf.eval("e", null));
    }

    @Test
    void scoresEventsThroughSqlWithRegisteredFunction() throws Exception {
        JobConfig config = JobConfig.resolve(
                new String[] {
                    "--kafka.bootstrap.servers", "b-1.example:9096",
                    "--kafka.username", "rtmpack",
                    "--kafka.password", "it's \"secret\"",
                    "--app.state.ttl.ms", "60000"
                },
                Map.of(),
                Map.of());

        String script = UdfSql.render(UdfSql.readResource(UdfSql.RESOURCE), config.values());
        assertTrue(!script.contains("${"), "all placeholders must be substituted");

        List<Vector> vectors = vectors();
        Instant ts = Instant.parse("2026-11-08T10:15:30.123Z");
        List<Row> rows = new ArrayList<>();
        long seq = 0;
        for (Vector v : vectors) {
            rows.add(Row.of(v.eventId(), v.key(), seq++, 1000L, false, "pad", ts));
        }

        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);
        StreamTableEnvironment tableEnv = StreamTableEnvironment.create(env);
        CALLS.set(0);
        tableEnv.createTemporarySystemFunction(ScoreUdf.NAME, CountingScoreUdf.class);

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
                .build());
        tableEnv.createTemporaryView("udf_input", input);

        String insert = null;
        String select = null;
        for (String statement : UdfSql.statements(script)) {
            if (SOURCE_DDL.matcher(statement).matches()) {
                continue;
            }
            if (UdfSql.isInsert(statement)) {
                insert = statement;
                select = UdfSql.insertSelect(statement);
            } else if (!UdfSql.applyIfSet(tableEnv, statement)) {
                tableEnv.executeSql(statement); // sink DDL
            }
        }
        assertTrue(select != null, "udf.sql must contain the INSERT statement");

        // The INSERT plans against the real Kafka sink options, and the operators are not
        // separated by a shuffle.
        String plan = tableEnv.explainSql(insert);
        assertFalse(plan.contains("Exchange"), "stateless scoring must not add an exchange:\n" + plan);
        assertTrue(plan.contains("score_event"), "plan must call the registered function:\n" + plan);

        List<Row> out = new ArrayList<>();
        try (CloseableIterator<Row> it = tableEnv.sqlQuery(select).execute().collect()) {
            it.forEachRemaining(out::add);
        }

        assertEquals(vectors.size(), out.size());
        assertEquals(vectors.size(), CALLS.get(), "score_event must run once per record");
        // Columns: kafka_key, event_id, key, seq, event_ts, score, decision, in_ts, engine
        Map<String, Row> byKey = new HashMap<>();
        for (Row r : out) {
            byKey.put((String) r.getField(0), r);
            assertEquals(r.getField(0), r.getField(1), "Kafka key is event_id");
            assertEquals("flink", r.getField(8));
            assertEquals(ts.toEpochMilli(), r.getField(7), "in_ts must be epoch millis");
            assertEquals(1000L, r.getField(4));
        }
        long seq2 = 0;
        for (Vector v : vectors) {
            Row r = byKey.get(v.eventId());
            assertTrue(r != null, "missing output for " + v.eventId());
            assertEquals(v.key(), r.getField(2));
            assertEquals(seq2++, r.getField(3));
            assertEquals(v.score(), r.getField(5), v.eventId());
            assertEquals(v.decision(), r.getField(6), v.eventId());
        }
    }

    @Test
    void sinkKeepsKeyOutOfValue() throws Exception {
        String sql = UdfSql.readResource(UdfSql.RESOURCE);
        assertTrue(sql.contains("'key.fields' = 'kafka_key'"));
        assertTrue(sql.contains("'value.fields-include' = 'EXCEPT_KEY'"));
        assertTrue(sql.contains("'properties.linger.ms' = '0'"));
        assertTrue(sql.contains("'sink.delivery-guarantee' = 'at-least-once'"));
    }
}
