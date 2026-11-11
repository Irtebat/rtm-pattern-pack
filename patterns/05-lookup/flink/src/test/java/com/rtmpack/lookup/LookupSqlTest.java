package com.rtmpack.lookup;

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
import java.util.List;
import java.util.Map;
import java.util.regex.Pattern;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * Runs the real statements from lookup.sql on a bounded in-memory source.
 *
 * <p>The source Kafka DDL is replaced by a view with the same columns. The function and sink DDL
 * are executed (they need no broker), so the final INSERT is planned against the real sink
 * options. The INSERT query is run as a SELECT with its rows collected. The reference table
 * comes from src/test/resources/merchants.csv, which shadows the packaged file on the test
 * classpath. Its first rows are (values from the contract rule):
 * m-00000 medium electronics GB, m-00001 high grocery GB, m-00002 high grocery IN.
 */
class LookupSqlTest {

    private static final Pattern SOURCE_DDL = Pattern.compile("(?is)^CREATE\\s+TABLE\\s+lookup_input\\b.*");

    @Test
    void enrichesKnownMerchantsAndDropsUnknownOnes() throws Exception {
        JobConfig config = JobConfig.resolve(
                new String[] {
                    "--kafka.bootstrap.servers", "b-1.example:9096",
                    "--kafka.username", "rtmpack",
                    "--kafka.password", "it's \"secret\"",
                    "--app.state.ttl.ms", "60000"
                },
                Map.of(),
                Map.of());

        String script = LookupSql.render(LookupSql.readResource(LookupSql.RESOURCE), config.values());
        assertTrue(!script.contains("${"), "all placeholders must be substituted");

        Instant ts1 = Instant.parse("2026-11-08T10:15:30.123Z");
        Instant ts2 = Instant.parse("2026-11-08T10:15:31.456Z");
        List<Row> rows = new ArrayList<>();
        rows.add(event("e-1", "user-1", "m-00000", ts1));
        rows.add(event("e-2", "user-2", "m-00001", ts2));
        rows.add(event("e-3", "user-3", "m-00002", ts2));
        rows.add(event("e-4", "user-4", "m-99999", ts1)); // unknown merchant
        rows.add(event("e-5", "user-5", null, ts1)); // missing merchant
        rows.add(event("e-6", "user-1", "m-00000", ts2)); // same merchant again

        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);
        StreamTableEnvironment tableEnv = StreamTableEnvironment.create(env);

        DataStream<Row> stream = env.fromData(rows, Types.ROW_NAMED(
                new String[] {"event_id", "key", "seq", "event_ts", "is_dup", "merchant_id", "payload", "in_ts_ltz"},
                Types.STRING, Types.STRING, Types.LONG, Types.LONG, Types.BOOLEAN, Types.STRING,
                Types.STRING, Types.INSTANT));
        Table input = tableEnv.fromDataStream(stream, Schema.newBuilder()
                .column("event_id", DataTypes.STRING())
                .column("key", DataTypes.STRING())
                .column("seq", DataTypes.BIGINT())
                .column("event_ts", DataTypes.BIGINT())
                .column("is_dup", DataTypes.BOOLEAN())
                .column("merchant_id", DataTypes.STRING())
                .column("payload", DataTypes.STRING())
                .column("in_ts_ltz", DataTypes.TIMESTAMP_LTZ(3))
                .build());
        tableEnv.createTemporaryView("lookup_input", input);

        String insert = null;
        String select = null;
        for (String statement : LookupSql.statements(script)) {
            if (SOURCE_DDL.matcher(statement).matches()) {
                continue;
            }
            if (LookupSql.isInsert(statement)) {
                insert = statement;
                select = LookupSql.insertSelect(statement);
            } else if (!LookupSql.applyIfSet(tableEnv, statement)) {
                tableEnv.executeSql(statement); // function and sink DDL
            }
        }
        assertTrue(select != null, "lookup.sql must contain the INSERT statement");

        // The INSERT plans against the real Kafka sink options. The lookup is local, so no
        // operator is separated by a shuffle.
        String plan = tableEnv.explainSql(insert);
        assertFalse(plan.contains("Exchange"), "lookup join must not add an exchange:\n" + plan);
        assertTrue(plan.contains("Correlate"), "lookup should run as a correlate:\n" + plan);

        List<Row> out = new ArrayList<>();
        try (CloseableIterator<Row> it = tableEnv.sqlQuery(select).execute().collect()) {
            it.forEachRemaining(out::add);
        }

        // Columns: kafka_key, event_id, key, event_ts, merchant_id, risk_tier, category, country, in_ts, engine
        assertEquals(4, out.size(), "unknown and missing merchants must produce no output");
        Map<String, Row> byId = new HashMap<>();
        for (Row r : out) {
            byId.put((String) r.getField(1), r);
            assertEquals(r.getField(1), r.getField(0), "Kafka key is event_id");
            assertEquals("flink", r.getField(9));
        }
        assertFalse(byId.containsKey("e-4"), "unknown merchant dropped");
        assertFalse(byId.containsKey("e-5"), "null merchant dropped");

        assertEnriched(byId.get("e-1"), "m-00000", "medium", "electronics", "GB", ts1);
        assertEnriched(byId.get("e-2"), "m-00001", "high", "grocery", "GB", ts2);
        assertEnriched(byId.get("e-3"), "m-00002", "high", "grocery", "IN", ts2);
        assertEnriched(byId.get("e-6"), "m-00000", "medium", "electronics", "GB", ts2);
        assertEquals("user-1", byId.get("e-1").getField(2));
        assertEquals(1000L, byId.get("e-1").getField(3));
    }

    @Test
    void lookupLoadsEveryFixtureRow() throws Exception {
        try (var in = MerchantLookup.class.getResourceAsStream(MerchantLookup.RESOURCE)) {
            Map<String, String[]> table = MerchantLookup.parse(in);
            assertEquals(100, table.size());
            assertNull(table.get("m-00100"));
        }
    }

    @Test
    void sinkKeepsKeyOutOfValue() throws Exception {
        String sql = LookupSql.readResource(LookupSql.RESOURCE);
        assertTrue(sql.contains("'key.fields' = 'kafka_key'"));
        assertTrue(sql.contains("'value.fields-include' = 'EXCEPT_KEY'"));
        assertTrue(sql.contains("'properties.linger.ms' = '0'"));
        assertTrue(sql.contains("'sink.delivery-guarantee' = 'at-least-once'"));
    }

    private static void assertEnriched(Row r, String merchant, String tier, String category, String country, Instant ts) {
        assertTrue(r != null, "missing output for " + merchant);
        assertEquals(merchant, r.getField(4));
        assertEquals(tier, r.getField(5));
        assertEquals(category, r.getField(6));
        assertEquals(country, r.getField(7));
        assertEquals(ts.toEpochMilli(), r.getField(8), "in_ts must be epoch millis");
    }

    private static Row event(String id, String key, String merchant, Instant ts) {
        return Row.of(id, key, 1L, 1000L, false, merchant, "pad", ts);
    }
}
