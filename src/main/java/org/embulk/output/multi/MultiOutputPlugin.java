package org.embulk.output.multi;

import org.embulk.config.Config;
import org.embulk.config.ConfigDefault;
import org.embulk.config.ConfigDiff;
import org.embulk.config.ConfigException;
import org.embulk.config.ConfigSource;
import org.embulk.config.Task;
import org.embulk.config.TaskReport;
import org.embulk.config.TaskSource;
import org.embulk.plugin.PluginType;
import org.embulk.spi.Buffer;
import org.embulk.spi.Exec;
import org.embulk.spi.ExecSession;
import org.embulk.spi.OutputPlugin;
import org.embulk.spi.Page;
import org.embulk.spi.Schema;
import org.embulk.spi.TransactionalPageOutput;

import java.util.ArrayList;
import java.util.List;
import java.util.Optional;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.Future;
import java.util.function.Function;
import java.util.stream.Collectors;

public class MultiOutputPlugin implements OutputPlugin {
    public interface PluginTask extends Task {
        @Config("outputs")
        List<ConfigSource> getOutputConfigs();

        @Config(CONFIG_NAME_OUTPUT_CONFIG_DIFFS)
        @ConfigDefault("null")
        Optional<List<ConfigDiff>> getOutputConfigDiffs();

        List<TaskSource> getTaskSources();
        void setTaskSources(List<TaskSource> taskSources);
    }

    private static final String CONFIG_NAME_OUTPUT_CONFIG_DIFFS = "output_config_diffs";
    static final String CONFIG_NAME_OUTPUT_TASK_REPORTS = "output_task_reports";

    @Override
    public ConfigDiff transaction(ConfigSource config, Schema schema, int taskCount, OutputPlugin.Control control) {
        final PluginTask task = config.loadConfig(PluginTask.class);
        if (task.getOutputConfigs().isEmpty()) {
            throw new ConfigException("'outputs' must have more than than or equals to 1 element.");
        }
        final ExecSession session = Exec.session();
        final RunControlTask controlTask = new RunControlTask(task, control);
        return buildConfigDiff(mapWithPluginDelegate(task, session, delegate ->
                delegate.transaction(schema, taskCount, controlTask)
        ));
    }

    @Override
    public ConfigDiff resume(TaskSource taskSource, Schema schema, int taskCount, OutputPlugin.Control control) {
        final PluginTask task = taskSource.loadTask(PluginTask.class);
        final ExecSession session = Exec.session();
        final RunControlTask controlTask = new RunControlTask(task, control);
        return buildConfigDiff(mapWithPluginDelegate(task, session, delegate ->
                delegate.resume(schema, taskCount, controlTask)
        ));
    }

    @Override
    public void cleanup(TaskSource taskSource, Schema schema, int taskCount, List<TaskReport> successTaskReports) {
        final PluginTask task = taskSource.loadTask(PluginTask.class);
        final ExecSession session = Exec.session();
        mapWithPluginDelegate(task, session, delegate -> {
            delegate.cleanup(schema, taskCount, successTaskReports);
            return null;
        });
    }

    @Override
    public TransactionalPageOutput open(TaskSource taskSource, Schema schema, int taskIndex) {
        final PluginTask task = taskSource.loadTask(PluginTask.class);
        final ExecSession session = Exec.session();
        final List<TransactionalPageOutputDelegate> delegates = mapWithPluginDelegate(task, session, delegate ->
                new TransactionalPageOutputDelegate(taskIndex, delegate, delegate.open(schema, taskIndex))
        );

        return new TransactionalPageOutput() {
            @Override
            public void add(Page original) {
                final Buffer originalBuffer = original.buffer();
                for (TransactionalPageOutputDelegate output : delegates) {
                    final Buffer copiedBuffer = Buffer.wrap(originalBuffer.array());
                    copiedBuffer.offset(originalBuffer.offset());
                    copiedBuffer.limit(originalBuffer.limit());

                    final Page copiedPage = Page.wrap(copiedBuffer);
                    copiedPage.setStringReferences(new ArrayList<>(original.getStringReferences()));
                    copiedPage.setValueReferences(new ArrayList<>(original.getValueReferences()));

                    output.add(copiedPage);
                }
            }

            @Override
            public void finish() {
                for (TransactionalPageOutputDelegate output : delegates) {
                    output.finish();
                }
            }

            @Override
            public void close() {
                for (TransactionalPageOutputDelegate output : delegates) {
                    output.close();
                }
            }

            @Override
            public void abort() {
                for (TransactionalPageOutputDelegate output : delegates) {
                    output.abort();
                }
            }

            @Override
            public TaskReport commit() {
                final TaskReport report = Exec.newTaskReport();
                final List<TaskReport> reports = new ArrayList<>();
                final List<OutputPluginDelegate> errorPlugins = new ArrayList<>();
                for (TransactionalPageOutputDelegate output : delegates) {
                    try {
                        reports.add(output.commit());
                    } catch (PluginExecutionException e) {
                        errorPlugins.add(e.getPlugin());
                    }
                }
                if (!errorPlugins.isEmpty()) {
                    throw new RuntimeException(
                            String.format("Following plugins failed to output [%s]",
                                    errorPlugins.stream().map(OutputPluginDelegate::getPluginCode).collect(Collectors.joining(", "))
                            ));
                }
                report.set(CONFIG_NAME_OUTPUT_TASK_REPORTS, new TaskReports(reports));
                return report;
            }
        };
    }

    private static ConfigDiff buildConfigDiff(List<Future<ConfigDiff>> runPluginTasks) {
        final ConfigDiff configDiff = Exec.newConfigDiff();
        List<ConfigDiff> configDiffs = new ArrayList<>();
        for (Future<ConfigDiff> pluginTask : runPluginTasks) {
            try {
                configDiffs.add(pluginTask.get());
            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
                throw new RuntimeException(e);
            } catch (ExecutionException e) {
                throw new RuntimeException(e.getCause());
            }
        }
        configDiff.set(CONFIG_NAME_OUTPUT_CONFIG_DIFFS, configDiffs);
        return configDiff;
    }

    private <T> List<T> mapWithPluginDelegate(PluginTask task, ExecSession session, Function<OutputPluginDelegate, T> action) {
        List<T> result = new ArrayList<>();
        for (int pluginIndex = 0; pluginIndex < task.getOutputConfigs().size(); pluginIndex++) {
            final ConfigSource config = task.getOutputConfigs().get(pluginIndex);
            if (task.getOutputConfigDiffs().isPresent()) {
                config.merge(task.getOutputConfigDiffs().get().get(pluginIndex));
            }
            final PluginType pluginType = config.get(PluginType.class, "type");
            final OutputPlugin outputPlugin = session.newPlugin(OutputPlugin.class, pluginType);
            TaskSource taskSource = null;
            if (task.getTaskSources() != null) {
                taskSource = task.getTaskSources().get(pluginIndex);
            }
            result.add(action.apply(new OutputPluginDelegate(pluginIndex, pluginType, outputPlugin, config, taskSource)));
        }
        return result;
    }
}
