package com.rtmpack.unnest;

import org.apache.flink.api.common.typeinfo.TypeInformation;
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
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * Runs the real statements from unnest.sql on a bounded in-memory source.
 *
 * <p>The source Kafka DDL is replaced by a view with the same columns. The sink DDL is executed
 * (it needs no broker) so the final INSERT is planned against the real sink options. The INSERT
 * query is run as a SELECT with its rows collected.
 */
class UnnestSqlTest {

    private static final Pattern SOURCE_DDL = Pattern.compile("(?is)^CREATE\\s+TABLE\\s+unnest_input\\b.*");

    @Test
    void unnestsItemsWithZeroBasedIndexAndKey() throws Exception {
        JobConfig config = JobConfig.resolve(
                new String[] {
                    "--kafka.bootstrap.servers", "b-1.example:9096",
                    "--kafka.username", "rtmpack",
                    "--kafka.password", "it's \"secret\"",
                    "--app.state.ttl.ms", "60000"
                },
                Map.of(),
                Map.of());

        String script = UnnestSql.render(UnnestSql.readResource(UnnestSql.RESOURCE), config.values());
        assertTrue(!script.contains("${"), "all placeholders must be substituted");

        Instant ts1 = Instant.parse("2026-11-08T10:15:30.123Z");
        Instant ts2 = Instant.parse("2026-11-08T10:15:31.456Z");
        List<Row> rows = new ArrayList<>();
        rows.add(order("o-1", "cust-1", ts1, Row.of("A", 1, 1.5), Row.of("B", 2, 2.5), Row.of("C", 3, 3.5)));
        rows.add(order("o-2", "cust-2", ts2, Row.of("D", 4, 4.5)));
        rows.add(order("o-3", "cust-3", ts2, Row.of("E", 5, 5.5), Row.of("F", 6, 6.5)));
        int expected = 3 + 1 + 2;

        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);
        StreamTableEnvironment tableEnv = StreamTableEnvironment.create(env);

        TypeInformation<Row> itemType = Types.ROW_NAMED(
                new String[] {"sku", "qty", "price"}, Types.STRING, Types.INT, Types.DOUBLE);
        DataStream<Row> stream = env.fromData(rows, Types.ROW_NAMED(
                new String[] {"order_id", "key", "event_ts", "items", "payload", "in_ts_ltz"},
                Types.STRING, Types.STRING, Types.LONG, Types.OBJECT_ARRAY(itemType), Types.STRING, Types.INSTANT));
        Table input = tableEnv.fromDataStream(stream, Schema.newBuilder()
                .column("order_id", DataTypes.STRING())
                .column("key", DataTypes.STRING())
                .column("event_ts", DataTypes.BIGINT())
                .column("items", DataTypes.ARRAY(DataTypes.ROW(
                        DataTypes.FIELD("sku", DataTypes.STRING()),
                        DataTypes.FIELD("qty", DataTypes.INT()),
                        DataTypes.FIELD("price", DataTypes.DOUBLE()))))
                .column("payload", DataTypes.STRING())
                .column("in_ts_ltz", DataTypes.TIMESTAMP_LTZ(3))
                .build());
        tableEnv.createTemporaryView("unnest_input", input);

        String insert = null;
        String select = null;
        for (String statement : UnnestSql.statements(script)) {
            if (SOURCE_DDL.matcher(statement).matches()) {
                continue;
            }
            if (UnnestSql.isInsert(statement)) {
                insert = statement;
                select = UnnestSql.insertSelect(statement);
            } else if (!UnnestSql.applyIfSet(tableEnv, statement)) {
                tableEnv.executeSql(statement); // sink DDL
            }
        }
        assertTrue(select != null, "unnest.sql must contain the INSERT statement");

        // The INSERT plans against the real Kafka sink options, and the operators are not
        // separated by a shuffle.
        String plan = tableEnv.explainSql(insert);
        assertFalse(plan.contains("Exchange"), "stateless unnest must not add an exchange:\n" + plan);

        List<Row> out = new ArrayList<>();
        try (CloseableIterator<Row> it = tableEnv.sqlQuery(select).execute().collect()) {
            it.forEachRemaining(out::add);
        }

        assertEquals(expected, out.size(), "row count must equal the sum of items");
        // Columns: kafka_key, order_id, key, event_ts, item_idx, sku, qty, price, in_ts, engine
        Map<String, Row> byKey = new HashMap<>();
        for (Row r : out) {
            byKey.put((String) r.getField(0), r);
            assertEquals("flink", r.getField(9));
            assertEquals(r.getField(1) + ":" + r.getField(4), r.getField(0), "key format");
        }
        assertEquals(expected, byKey.size(), "keys must be distinct");

        Row first = byKey.get("o-1:0");
        Row third = byKey.get("o-1:2");
        assertTrue(first != null && third != null && byKey.containsKey("o-1:1"), "idx must be 0-based and dense");
        assertEquals(0, first.getField(4));
        assertEquals("A", first.getField(5));
        assertEquals(1, first.getField(6));
        assertEquals(1.5, first.getField(7));
        assertEquals("C", third.getField(5));
        assertEquals(3.5, third.getField(7));
        assertEquals("cust-1", first.getField(2));
        assertEquals(1000L, first.getField(3));
        assertEquals(ts1.toEpochMilli(), first.getField(8), "in_ts must be epoch millis");
        assertEquals(ts2.toEpochMilli(), byKey.get("o-2:0").getField(8));
        assertEquals(ts2.toEpochMilli(), byKey.get("o-3:1").getField(8));
        assertFalse(byKey.containsKey("o-2:1"), "o-2 has one item");
    }

    @Test
    void sinkKeepsKeyOutOfValue() throws Exception {
        String sql = UnnestSql.readResource(UnnestSql.RESOURCE);
        assertTrue(sql.contains("'key.fields' = 'kafka_key'"));
        assertTrue(sql.contains("'value.fields-include' = 'EXCEPT_KEY'"));
        assertTrue(sql.contains("'properties.linger.ms' = '0'"));
        assertTrue(sql.contains("'sink.delivery-guarantee' = 'at-least-once'"));
    }

    private static Row order(String id, String key, Instant ts, Row... items) {
        return Row.of(id, key, 1000L, items, "pad", ts);
    }
}
