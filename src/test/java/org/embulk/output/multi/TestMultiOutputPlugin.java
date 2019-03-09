package org.embulk.output.multi;

import org.embulk.config.ConfigSource;
import org.embulk.exec.PartialExecutionException;
import org.embulk.test.EmbulkPluginTest;
import org.embulk.test.EmbulkTest;
import org.embulk.test.Record;
import org.embulk.test.TestingEmbulk;
import org.junit.jupiter.api.Test;

import java.io.BufferedReader;
import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.InputStreamReader;
import java.io.PrintStream;
import java.util.Arrays;
import java.util.Set;

import static java.util.stream.Collectors.joining;
import static java.util.stream.Collectors.toSet;
import static org.embulk.test.LocalObjectOutputPlugin.assertRecords;
import static org.embulk.test.Utils.configFromResource;
import static org.embulk.test.Utils.record;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNull;

@EmbulkTest(MultiOutputPlugin.class)
class TestMultiOutputPlugin extends EmbulkPluginTest {

    @Test
    void testMultipleOutputWorking() throws IOException {
        final ConfigSource inConfig = configFromResource("yaml/in_base.yml");
        final ConfigSource outConfig = configFromResource("yaml/out_base.yml");
        final ByteArrayOutputStream outputStream = new ByteArrayOutputStream();
        System.setOut(new PrintStream(outputStream));

        runOutput(inConfig, outConfig);

        // Check local_object output plugin working
        final Record[] values = new Record[]{record(1, "user1", 20), record(2, "user2", 21)};
        assertRecords(values);

        // Check stdout output plugin working
        final BufferedReader reader = new BufferedReader(
                new InputStreamReader(new ByteArrayInputStream(outputStream.toByteArray()))
        );
        // Header
        assertEquals("id,name,age", reader.readLine());
        // Values
        Set<String> expected = Arrays.stream(values)
                .map(v -> v.getValues().stream().map(Object::toString).collect(joining(",")))
                .collect(toSet());
        Set<String> actual = reader.lines().collect(toSet());
        assertEquals(expected, actual);
    }

    @Test
    void testMultipleOutputWorkingWithBigRecords() throws IOException {
        final ConfigSource inConfig = configFromResource("yaml/in_base.yml");
        // Set 1000 records
        Object[][][] values = generateValues(1000);
        inConfig.set("values", values);

        final ConfigSource outConfig = configFromResource("yaml/out_base.yml");
        final ByteArrayOutputStream outputStream = new ByteArrayOutputStream();
        System.setOut(new PrintStream(outputStream));

        runOutput(inConfig, outConfig);

        // Check local_object output plugin working
        assertRecords(toRecords(values));

        // Check stdout output plugin working
        final BufferedReader reader = new BufferedReader(
                new InputStreamReader(new ByteArrayInputStream(outputStream.toByteArray()))
        );
        // Header
        assertEquals("id,name,age", reader.readLine());
        // Values
        Set<String> expected = Arrays.stream(values).map(v -> String.format("%s,%s,%s", v[0][0], v[0][1], v[0][2])).collect(toSet());
        Set<String> actual = reader.lines().collect(toSet());
        assertEquals(expected, actual);
    }

    @Test
    void testOutputConfigDiffs() {
        final ConfigSource inConfig = configFromResource("yaml/in_base.yml");
        // Set 3 records
        Object[][][] values = generateValues(3);
        inConfig.set("values", values);

        // Set incremental column
        final ConfigSource outConfig = configFromResource("yaml/out_base.yml");

        outConfig.set("incremental", true);
        outConfig.set("incremental_column", "id");

        // Run Embulk
        TestingEmbulk.RunResult result = runOutput(inConfig, outConfig);

        // All records should be loaded.
        assertRecords(toRecords(values));

        // Set 4 records
        values = generateValues(4);
        inConfig.set("values", values);

        // Rerun with config diff
        runOutput(inConfig, outConfig, result.getConfigDiff());

        // Only 1 records (id=4) should be loaded.
        final Record[] records = toRecords(values);
        assertRecords(records[3]);
    }

    @Test
    void testAnOutputFailedAfterOpen() throws IOException {
        final ConfigSource inConfig = configFromResource("yaml/in_base.yml");
        final ConfigSource outConfig = configFromResource("yaml/out_failed_output_after_open.yml");
        final ByteArrayOutputStream outputStream = new ByteArrayOutputStream();
        System.setOut(new PrintStream(outputStream));

        // Run Embulk without error
        try {
            runOutput(inConfig, outConfig);
        } catch (PartialExecutionException ignored) {}

        // All records should be loaded for local_object output plugin.
        final Record[] values = new Record[]{record(1, "user1", 20), record(2, "user2", 21)};
        assertRecords(values);

        // No record should be loaded for stdout output plugin
        final BufferedReader reader = new BufferedReader(
                new InputStreamReader(new ByteArrayInputStream(outputStream.toByteArray()))
        );
        assertNull(reader.readLine());
    }

    @Test
    void testAnOutputFailedBeforeOpen() throws IOException {
        final ConfigSource inConfig = configFromResource("yaml/in_base.yml");
        final ConfigSource outConfig = configFromResource("yaml/out_failed_output_before_open.yml");
        final ByteArrayOutputStream outputStream = new ByteArrayOutputStream();
        System.setOut(new PrintStream(outputStream));

        // Run Embulk without error
        try {
            runOutput(inConfig, outConfig);
        } catch (PartialExecutionException ignored) {}

        // No record should be loaded for local_object output plugin.
        assertRecords();

        // No record should be loaded for stdout output plugin
        final BufferedReader reader = new BufferedReader(
                new InputStreamReader(new ByteArrayInputStream(outputStream.toByteArray()))
        );
        assertNull(reader.readLine());
    }

    private static Object[][][] generateValues(int size) {
        Object[][][] values = new Object[size][1][3];
        for (int i = 0; i < size; i++) {
            values[i][0][0] = i + 1;               // ID
            values[i][0][1] = "user" + (i + 1);    // Name
            values[i][0][2] = 20;                  // Age
        }
        return values;
    }

    private static Record[] toRecords(Object[][][] values) {
        return Arrays.stream(values).map(v -> record(v[0])).toArray(Record[]::new);
    }
}
