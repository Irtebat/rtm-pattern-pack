package com.rtmpack.join;

import org.apache.flink.api.common.typeinfo.Types;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.DataTypes;
import org.apache.flink.table.api.Schema;
import org.apache.flink.table.api.Table;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;
import org.apache.flink.types.Row;
import org.apache.flink.util.CloseableIterator;
import org.junit.jupiter.api.BeforeEach;
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
 * Runs the real statements from join.sql on bounded in-memory sources.
 *
 * <p>The three Kafka DDL statements are not executed. The source tables are replaced by views
 * with the same columns, the same rowtime expression and the same watermark, and the INSERT query
 * is run as a SELECT with its rows collected. Everything else (session settings, the interval
 * join condition, the in_ts conversion) is the production SQL.
 */
class JoinSqlTest {

    private static final Pattern KAFKA_DDL = Pattern.compile("(?is)^CREATE\\s+TABLE\\s+join_(orders|payments|output)\\b.*");
    private static final long T0 = 1_800_000_000_000L;

    private StreamTableEnvironment tableEnv;
    private StreamExecutionEnvironment env;
    private String select;

    @BeforeEach
    void setUp() throws Exception {
        JobConfig config = JobConfig.resolve(
                new String[] {
                    "--kafka.bootstrap.servers", "b-1.example:9096",
                    "--kafka.username", "rtmpack",
                    "--kafka.password", "it's \"secret\""
                },
                Map.of(),
                Map.of());
        String script = JoinSql.render(JoinSql.readResource(JoinSql.RESOURCE), config.values());
        assertFalse(script.contains("${"), "all placeholders must be substituted");
        assertTrue(script.contains("rtmpack.08-join.input.orders") && script.contains("rtmpack.08-join.input.payments"),
                "source topics are the input base plus .orders and .payments");

        env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);
        tableEnv = StreamTableEnvironment.create(env);

        for (String statement : JoinSql.statements(script)) {
            if (KAFKA_DDL.matcher(statement).matches()) {
                continue;
            }
            if (JoinSql.isInsert(statement)) {
                select = JoinSql.insertSelect(statement);
            } else {
                assertTrue(JoinSql.applyIfSet(tableEnv, statement), "unexpected statement: " + statement);
            }
        }
        assertNotNull(select, "join.sql must contain the INSERT statement");
    }

    @Test
    void planUsesIntervalJoinNotRegularJoin() throws Exception {
        register(List.of(order("o1", T0, Instant.ofEpochMilli(T0))),
                List.of(payment("p1", "o1", T0 + 1000, Instant.ofEpochMilli(T0 + 1000))));

        String plan = tableEnv.sqlQuery(select).explain();

        assertTrue(plan.contains("IntervalJoin"), "plan must contain IntervalJoin:\n" + plan);
        // The logical plan has a LogicalJoin node; only the optimized plan decides the operator.
        String optimized = plan.substring(plan.indexOf("== Optimized Physical Plan =="));
        assertFalse(Pattern.compile("(?<![A-Za-z])Join\\(").matcher(optimized).find(),
                "plan must not contain a regular Join:\n" + plan);
    }

    @Test
    void joinBoundariesAndOutputFields() throws Exception {
        Instant inO = Instant.parse("2026-11-08T10:15:30.123Z");
        Instant inP = Instant.parse("2026-11-08T10:15:31.456Z");
        List<Row> orders = new ArrayList<>();
        List<Row> payments = new ArrayList<>();
        // Payment at +0 s, +30 s: included. Payment at +30.001 s and 1 ms before the order: excluded.
        for (String id : List.of("at0", "at30", "at30p1", "before")) {
            orders.add(order(id, T0, inO));
        }
        payments.add(payment("pay-at0", "at0", T0, inP));
        payments.add(payment("pay-at30", "at30", T0 + 30_000, inP));
        payments.add(payment("pay-at30p1", "at30p1", T0 + 30_001, inP));
        payments.add(payment("pay-before", "before", T0 - 1, inP));
        // Payment with an order id that has no order, and an order without a payment.
        payments.add(payment("pay-orphan", "nobody", T0, inP));
        orders.add(order("unpaid", T0, inO));
        // Payment for a different order id does not match even when inside the window.
        orders.add(order("other", T0, inO));
        payments.add(payment("pay-wrong", "at0-x", T0 + 5, inP));

        register(orders, payments);
        Map<String, Row> byOrder = new HashMap<>();
        List<Row> out = collect();
        for (Row r : out) {
            byOrder.put((String) r.getField(0), r);
        }

        assertEquals(2, out.size(), "only the +0 s and +30 s payments match: " + out);
        assertEquals(java.util.Set.of("at0", "at30"), byOrder.keySet());

        Row r = byOrder.get("at30");
        assertEquals("pay-at30", r.getField(1));
        assertEquals("cust-7", r.getField(2));
        assertEquals(1234L, r.getField(3));
        assertEquals("card", r.getField(4));
        assertEquals(T0, r.getField(5));
        assertEquals(T0 + 30_000, r.getField(6));
        assertEquals(inP.toEpochMilli(), r.getField(7), "in_ts is the larger input in_ts");
        assertEquals("flink", r.getField(8));
    }

    @Test
    void inTsIsTheLargerOfTheTwoInputTimestamps() throws Exception {
        Instant early = Instant.parse("2026-11-08T10:15:30.001Z");
        Instant late = Instant.parse("2026-11-08T10:15:30.999Z");
        // Order arrives last in one pair and first in the other.
        register(List.of(order("a", T0, late), order("b", T0, early)),
                List.of(payment("pa", "a", T0 + 10, early), payment("pb", "b", T0 + 10, late)));

        List<Row> out = collect();

        assertEquals(2, out.size());
        for (Row r : out) {
            assertEquals(late.toEpochMilli(), r.getField(7));
        }
    }

    private List<Row> collect() throws Exception {
        List<Row> out = new ArrayList<>();
        try (CloseableIterator<Row> it = tableEnv.sqlQuery(select).execute().collect()) {
            it.forEachRemaining(out::add);
        }
        return out;
    }

    private void register(List<Row> orders, List<Row> payments) {
        DataStream<Row> o = env.fromData(orders, Types.ROW_NAMED(
                new String[] {"order_id", "key", "event_ts", "amount", "payload", "in_ts_ltz"},
                Types.STRING, Types.STRING, Types.LONG, Types.LONG, Types.STRING, Types.INSTANT));
        DataStream<Row> p = env.fromData(payments, Types.ROW_NAMED(
                new String[] {"payment_id", "order_id", "event_ts", "method", "payload", "in_ts_ltz"},
                Types.STRING, Types.STRING, Types.LONG, Types.STRING, Types.STRING, Types.INSTANT));
        Table ot = tableEnv.fromDataStream(o, Schema.newBuilder()
                .column("order_id", DataTypes.STRING())
                .column("key", DataTypes.STRING())
                .column("event_ts", DataTypes.BIGINT())
                .column("amount", DataTypes.BIGINT())
                .column("payload", DataTypes.STRING())
                .column("in_ts_ltz", DataTypes.TIMESTAMP_LTZ(3))
                .columnByExpression("event_time", "TO_TIMESTAMP_LTZ(event_ts, 3)")
                .watermark("event_time", "event_time - INTERVAL '5' SECOND")
                .build());
        Table pt = tableEnv.fromDataStream(p, Schema.newBuilder()
                .column("payment_id", DataTypes.STRING())
                .column("order_id", DataTypes.STRING())
                .column("event_ts", DataTypes.BIGINT())
                .column("method", DataTypes.STRING())
                .column("payload", DataTypes.STRING())
                .column("in_ts_ltz", DataTypes.TIMESTAMP_LTZ(3))
                .columnByExpression("event_time", "TO_TIMESTAMP_LTZ(event_ts, 3)")
                .watermark("event_time", "event_time - INTERVAL '5' SECOND")
                .build());
        tableEnv.createTemporaryView("join_orders", ot);
        tableEnv.createTemporaryView("join_payments", pt);
    }

    private static Row order(String id, long eventTs, Instant inTs) {
        return Row.of(id, "cust-7", eventTs, 1234L, "pad", inTs);
    }

    private static Row payment(String id, String orderId, long eventTs, Instant inTs) {
        return Row.of(id, orderId, eventTs, "card", "pad", inTs);
    }
}
