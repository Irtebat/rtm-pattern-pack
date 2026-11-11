package com.rtmpack.lookup;

import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.Properties;

/**
 * Resolved job parameters, keyed by flat names such as {@code kafka.bootstrap.servers}.
 *
 * <p>Sources, highest precedence first:
 * <ol>
 *   <li>Managed Flink runtime property groups. Group {@code kafka} key {@code bootstrap.servers}
 *       becomes {@code kafka.bootstrap.servers}; group {@code app} key {@code input.topic}
 *       becomes {@code app.input.topic}. {@code app.state.ttl.ms} is accepted and ignored.</li>
 *   <li>Command-line arguments in the form {@code --name value} or {@code --name=value}.</li>
 *   <li>Environment variables named {@code RTMPACK_} followed by the flat name in upper case with
 *       dots replaced by underscores, for example {@code RTMPACK_KAFKA_USERNAME}.</li>
 *   <li>Defaults for the application group.</li>
 * </ol>
 */
public final class JobConfig {

    static final String ENV_PREFIX = "RTMPACK_";

    static final String KAFKA_BOOTSTRAP = "kafka.bootstrap.servers";
    static final String KAFKA_USERNAME = "kafka.username";
    static final String KAFKA_PASSWORD = "kafka.password";
    static final String KAFKA_JAAS = "kafka.jaas.config";
    /** Accepted and ignored: the pattern is stateless. The Terraform module passes it for every pattern. */
    static final String APP_STATE_TTL_MS = "app.state.ttl.ms";
    static final String APP_INPUT_TOPIC = "app.input.topic";
    static final String APP_OUTPUT_TOPIC = "app.output.topic";

    static final List<String> REQUIRED = List.of(KAFKA_BOOTSTRAP, KAFKA_USERNAME, KAFKA_PASSWORD);

    private static final Map<String, String> DEFAULTS = Map.of(
            APP_INPUT_TOPIC, "rtmpack.05-lookup.input",
            APP_OUTPUT_TOPIC, "rtmpack.05-lookup.out.flink");

    private static final List<String> KNOWN_KEYS = List.of(
            KAFKA_BOOTSTRAP, KAFKA_USERNAME, KAFKA_PASSWORD,
            APP_STATE_TTL_MS, APP_INPUT_TOPIC, APP_OUTPUT_TOPIC);

    private final Map<String, String> values;

    private JobConfig(Map<String, String> values) {
        this.values = Collections.unmodifiableMap(values);
    }

    /** All resolved values, including the derived SASL JAAS string. */
    public Map<String, String> values() {
        return values;
    }

    public String get(String key) {
        return values.get(key);
    }

    /**
     * Resolves the configuration from all sources.
     *
     * @param args              command-line arguments, may be empty
     * @param runtimeProperties property groups from KinesisAnalyticsRuntime, may be empty
     * @param env               environment variables, usually {@code System.getenv()}
     * @throws IllegalArgumentException if a required value is missing or a value is malformed
     */
    public static JobConfig resolve(String[] args,
                                    Map<String, Properties> runtimeProperties,
                                    Map<String, String> env) {
        Map<String, String> merged = new HashMap<>(DEFAULTS);

        for (String key : KNOWN_KEYS) {
            String envValue = env.get(envName(key));
            if (envValue != null) {
                merged.put(key, envValue);
            }
        }

        merged.putAll(parseArgs(args));

        for (Map.Entry<String, Properties> group : runtimeProperties.entrySet()) {
            Properties properties = group.getValue();
            if (properties == null) {
                continue;
            }
            for (String name : properties.stringPropertyNames()) {
                merged.put(group.getKey() + "." + name, properties.getProperty(name));
            }
        }

        for (String key : REQUIRED) {
            String value = merged.get(key);
            if (value == null || value.isBlank()) {
                throw new IllegalArgumentException("Missing required parameter: " + key
                        + " (runtime property kafka/" + key.substring("kafka.".length())
                        + ", argument --" + key + ", or environment " + envName(key) + ")");
            }
        }

        merged.put(KAFKA_JAAS, scramJaas(merged.get(KAFKA_USERNAME), merged.get(KAFKA_PASSWORD)));
        return new JobConfig(merged);
    }

    static String envName(String key) {
        return ENV_PREFIX + key.toUpperCase(Locale.ROOT).replace('.', '_');
    }

    static Map<String, String> parseArgs(String[] args) {
        Map<String, String> parsed = new HashMap<>();
        int i = 0;
        while (i < args.length) {
            String arg = args[i];
            if (!arg.startsWith("--")) {
                throw new IllegalArgumentException("Expected --name option, got: " + arg);
            }
            String body = arg.substring(2);
            int eq = body.indexOf('=');
            if (eq >= 0) {
                parsed.put(body.substring(0, eq), body.substring(eq + 1));
                i++;
            } else {
                if (i + 1 >= args.length) {
                    throw new IllegalArgumentException("Missing value for --" + body);
                }
                parsed.put(body, args[i + 1]);
                i += 2;
            }
        }
        return parsed;
    }

    /** SASL/SCRAM-SHA-512 login module entry for the Kafka client. Quotes and backslashes are escaped. */
    static String scramJaas(String username, String password) {
        return "org.apache.kafka.common.security.scram.ScramLoginModule required username=\""
                + jaasEscape(username) + "\" password=\"" + jaasEscape(password) + "\";";
    }

    private static String jaasEscape(String value) {
        return value.replace("\\", "\\\\").replace("\"", "\\\"");
    }
}
