package com.rtmpack.join;

import com.amazonaws.services.kinesisanalytics.runtime.KinesisAnalyticsRuntime;
import org.apache.flink.table.api.EnvironmentSettings;
import org.apache.flink.table.api.TableEnvironment;
import org.apache.flink.table.api.TableResult;

import java.io.IOException;
import java.util.Map;
import java.util.Properties;

/**
 * Pattern 08-join: inner stream-stream join (orders and payments) as a Flink SQL application.
 *
 * <p>The SQL in {@code join.sql} defines the two Kafka sources and the Kafka sink and runs an
 * interval join on {@code order_id}. This class only resolves parameters, renders the script and
 * executes its statements in order.
 */
public final class JoinJob {

    private JoinJob() {
    }

    public static void main(String[] args) throws Exception {
        JobConfig config = JobConfig.resolve(args, runtimeProperties(), System.getenv());
        String script = JoinSql.render(JoinSql.readResource(JoinSql.RESOURCE), config.values());

        // Flink runtime settings such as execution.buffer-timeout are not changed here:
        // Managed Flink does not let application code change them. They can only be changed
        // through an AWS support case.
        TableEnvironment tableEnv = TableEnvironment.create(EnvironmentSettings.inStreamingMode());
        for (String statement : JoinSql.statements(script)) {
            if (JoinSql.applyIfSet(tableEnv, statement)) {
                continue;
            }
            TableResult result = tableEnv.executeSql(statement);
            if (JoinSql.isInsert(statement)) {
                // Blocks for the lifetime of the job. Managed Flink keeps the application running.
                result.await();
            }
        }
    }

    /**
     * Property groups from the Managed Flink runtime. Outside Managed Flink the properties file is
     * absent and the method fails, so an empty map is returned and CLI arguments or environment
     * variables are used instead.
     */
    static Map<String, Properties> runtimeProperties() {
        try {
            Map<String, Properties> groups = KinesisAnalyticsRuntime.getApplicationProperties();
            return groups == null ? Map.of() : groups;
        } catch (IOException | RuntimeException e) {
            return Map.of();
        }
    }
}
