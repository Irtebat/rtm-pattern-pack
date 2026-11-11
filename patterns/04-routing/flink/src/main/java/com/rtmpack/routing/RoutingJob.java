package com.rtmpack.routing;

import com.amazonaws.services.kinesisanalytics.runtime.KinesisAnalyticsRuntime;
import org.apache.flink.table.api.EnvironmentSettings;
import org.apache.flink.table.api.StatementSet;
import org.apache.flink.table.api.TableEnvironment;
import org.apache.flink.table.api.TableResult;

import java.io.IOException;
import java.util.Map;
import java.util.Properties;

/**
 * Pattern 04-routing: demux and fan-out as a Flink SQL application.
 *
 * <p>The SQL in {@code routing.sql} defines one Kafka source table, five Kafka sink tables (four
 * type topics and the all-events topic) and five {@code INSERT} statements. This class resolves
 * parameters, renders the script, executes the DDL and SET statements in order, and submits all
 * INSERT statements as one statement set, so the five sinks share one job and one source.
 */
public final class RoutingJob {

    private RoutingJob() {
    }

    public static void main(String[] args) throws Exception {
        JobConfig config = JobConfig.resolve(args, runtimeProperties(), System.getenv());
        String script = RoutingSql.render(RoutingSql.readResource(RoutingSql.RESOURCE), config.values());

        // Flink runtime settings such as execution.buffer-timeout are not changed here:
        // Managed Flink does not let application code change them. They can only be changed
        // through an AWS support case.
        TableEnvironment tableEnv = TableEnvironment.create(EnvironmentSettings.inStreamingMode());
        StatementSet statementSet = tableEnv.createStatementSet();
        for (String statement : RoutingSql.statements(script)) {
            if (RoutingSql.applyIfSet(tableEnv, statement)) {
                continue;
            }
            if (RoutingSql.isInsert(statement)) {
                statementSet.addInsertSql(statement);
            } else {
                tableEnv.executeSql(statement);
            }
        }
        TableResult result = statementSet.execute();
        // Blocks for the lifetime of the job. Managed Flink keeps the application running.
        result.await();
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
