package com.rtmpack.join;

import java.io.IOException;
import java.io.InputStream;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

/** Loads, renders and splits the join SQL script. */
public final class JoinSql {

    static final String RESOURCE = "/join.sql";

    private static final Pattern PLACEHOLDER = Pattern.compile("\\$\\{([^}]+)}");
    private static final Pattern INSERT = Pattern.compile("(?is)^\\s*INSERT\\s+INTO\\s+\\S+\\s+(.*)$");
    private static final Pattern SET = Pattern.compile("(?is)^\\s*SET\\s+'([^']+)'\\s*=\\s*'((?:[^']|'')*)'\\s*$");

    private JoinSql() {
    }

    public static String readResource(String name) throws IOException {
        try (InputStream in = JoinSql.class.getResourceAsStream(name)) {
            if (in == null) {
                throw new IOException("Resource not found on classpath: " + name);
            }
            return new String(in.readAllBytes(), StandardCharsets.UTF_8);
        }
    }

    /**
     * Replaces every placeholder with its value. Values are escaped for single-quoted SQL literals.
     *
     * @throws IllegalStateException if a placeholder has no value
     */
    public static String render(String template, Map<String, String> values) {
        Matcher matcher = PLACEHOLDER.matcher(template);
        StringBuilder out = new StringBuilder();
        while (matcher.find()) {
            String key = matcher.group(1);
            String value = values.get(key);
            if (value == null) {
                throw new IllegalStateException("No value for placeholder " + key);
            }
            matcher.appendReplacement(out, Matcher.quoteReplacement(value.replace("'", "''")));
        }
        matcher.appendTail(out);
        return out.toString();
    }

    /**
     * Splits a script into statements. Whole-line comments are removed first. A semicolon ends a
     * statement only when it is outside a single-quoted literal, so JAAS strings are kept intact.
     */
    public static List<String> statements(String script) {
        StringBuilder withoutComments = new StringBuilder();
        for (String line : script.split("\\R")) {
            if (!line.trim().startsWith("--")) {
                withoutComments.append(line).append('\n');
            }
        }

        List<String> statements = new ArrayList<>();
        StringBuilder current = new StringBuilder();
        boolean inLiteral = false;
        for (char c : withoutComments.toString().toCharArray()) {
            if (c == '\'') {
                inLiteral = !inLiteral;
            }
            if (c == ';' && !inLiteral) {
                addIfNotBlank(statements, current);
                current.setLength(0);
            } else {
                current.append(c);
            }
        }
        addIfNotBlank(statements, current);
        return statements;
    }

    /**
     * Applies a {@code SET 'key' = 'value'} statement to the table configuration. The Table API
     * does not accept SET through executeSql, so it is handled here.
     *
     * @return true if the statement was a SET statement
     */
    public static boolean applyIfSet(org.apache.flink.table.api.TableEnvironment tableEnv, String statement) {
        Matcher matcher = SET.matcher(statement);
        if (!matcher.matches()) {
            return false;
        }
        tableEnv.getConfig().getConfiguration().setString(matcher.group(1), matcher.group(2).replace("''", "'"));
        return true;
    }

    public static boolean isInsert(String statement) {
        return INSERT.matcher(statement).matches();
    }

    /** Returns the query part of an {@code INSERT INTO <table> <query>} statement. */
    public static String insertSelect(String insertStatement) {
        Matcher matcher = INSERT.matcher(insertStatement);
        if (!matcher.matches()) {
            throw new IllegalArgumentException("Not an INSERT INTO statement: " + insertStatement);
        }
        return matcher.group(1);
    }

    private static void addIfNotBlank(List<String> statements, StringBuilder text) {
        String trimmed = text.toString().trim();
        if (!trimmed.isEmpty()) {
            statements.add(trimmed);
        }
    }
}
