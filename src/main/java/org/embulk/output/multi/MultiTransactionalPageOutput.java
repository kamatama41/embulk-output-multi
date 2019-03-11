package org.embulk.output.multi;

import org.embulk.config.TaskReport;
import org.embulk.spi.Buffer;
import org.embulk.spi.Exec;
import org.embulk.spi.Page;
import org.embulk.spi.Schema;
import org.embulk.spi.TransactionalPageOutput;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.function.Consumer;
import java.util.stream.Collectors;

public class MultiTransactionalPageOutput implements TransactionalPageOutput {
    private static final Logger LOGGER = LoggerFactory.getLogger(MultiTransactionalPageOutput.class);
    private final int taskIndex;
    private final List<Delegate> delegates;

    static MultiTransactionalPageOutput open(Schema schema, int taskIndex, List<OutputPluginDelegate> plugins) {
        return new MultiTransactionalPageOutput(
                taskIndex,
                plugins.stream()
                        .map(plugin -> new Delegate(plugin, plugin.open(schema, taskIndex)))
                        .collect(Collectors.toList())
        );
    }

    private MultiTransactionalPageOutput(int taskIndex, List<Delegate> delegates) {
        this.taskIndex = taskIndex;
        this.delegates = delegates;
    }

    @Override
    public void add(Page page) {
        applyToAllPlugins(delegate -> delegate.add(copyPage(page)));
    }

    @Override
    public void finish() {
        applyToAllPlugins(Delegate::finish);
    }

    @Override
    public void close() {
        applyToAllPlugins(Delegate::close);
    }

    @Override
    public void abort() {
        applyToAllPlugins(Delegate::abort);
    }

    @Override
    public TaskReport commit() {
        final Map<String, TaskReport> reports = new HashMap<>();
        applyToAllPlugins(delegate -> reports.put(delegate.getTag(), delegate.commit()));
        final TaskReport report = Exec.newTaskReport();
        report.set(MultiOutputPlugin.CONFIG_NAME_OUTPUT_TASK_REPORTS, new TaskReports(reports));
        return report;
    }

    private void applyToAllPlugins(Consumer<Delegate> command) {
        final List<OutputPluginDelegate> errorPlugins = new ArrayList<>();
        for (Delegate delegate : delegates) {
            try {
                command.accept(delegate);
            } catch (Exception e) {
                LOGGER.warn(String.format("Output for %s on index %d failed.", delegate.plugin.getTag(), taskIndex), e);
                errorPlugins.add(delegate.plugin);
            }
        }

        if (!errorPlugins.isEmpty()) {
            throw new RuntimeException(
                    String.format("Following plugins failed to output [%s] on index %d",
                            errorPlugins.stream().map(OutputPluginDelegate::getTag).collect(Collectors.joining(", ")),
                            taskIndex
                    ));
        }
    }

    private static Page copyPage(Page original) {
        final Buffer originalBuffer = original.buffer();
        final Buffer copiedBuffer = Buffer.wrap(originalBuffer.array());
        copiedBuffer.offset(originalBuffer.offset());
        copiedBuffer.limit(originalBuffer.limit());

        final Page copiedPage = Page.wrap(copiedBuffer);
        copiedPage.setStringReferences(new ArrayList<>(original.getStringReferences()));
        copiedPage.setValueReferences(new ArrayList<>(original.getValueReferences()));
        return copiedPage;
    }

    static class Delegate implements TransactionalPageOutput {
        private final OutputPluginDelegate plugin;
        private final TransactionalPageOutput output;

        private Delegate(OutputPluginDelegate plugin, TransactionalPageOutput output) {
            this.plugin = plugin;
            this.output = output;
        }

        @Override
        public void add(Page page) {
            output.add(page);
        }

        @Override
        public void finish() {
            output.finish();
        }

        @Override
        public void close() {
            output.close();
        }

        @Override
        public void abort() {
            output.abort();
        }

        @Override
        public TaskReport commit() {
            return output.commit();
        }

        String getTag() {
            return plugin.getTag();
        }
    }
}
