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
import org.embulk.spi.Exec;
import org.embulk.spi.ExecSession;
import org.embulk.spi.OutputPlugin;
import org.embulk.spi.Schema;
import org.embulk.spi.TransactionalPageOutput;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.concurrent.ExecutionException;
import java.util.function.Function;

public class MultiOutputPlugin implements OutputPlugin {
    public interface PluginTask extends Task {
        @Config("outputs")
        List<ConfigSource> getOutputConfigs();

        @Config(CONFIG_NAME_OUTPUT_CONFIG_DIFFS)
        @ConfigDefault("null")
        Optional<Map<String, ConfigDiff>> getOutputConfigDiffs();

        Map<String, TaskSource> getTaskSources();
        void setTaskSources(Map<String, TaskSource> taskSources);
    }

    private static final Logger LOGGER = LoggerFactory.getLogger(MultiOutputPlugin.class);
    private static final String CONFIG_NAME_OUTPUT_CONFIG_DIFFS = "output_config_diffs";
    static final String CONFIG_NAME_OUTPUT_TASK_REPORTS = "output_task_reports";

    @Override
    public ConfigDiff transaction(ConfigSource config, Schema schema, int taskCount, OutputPlugin.Control control) {
        final PluginTask task = config.loadConfig(PluginTask.class);
        if (task.getOutputConfigs().isEmpty()) {
            throw new ConfigException("'outputs' must have more than or equals to 1 element.");
        }
        final ExecSession session = Exec.session();
        final AsyncRunControl runControl = AsyncRunControl.start(task, control);
        return buildConfigDiff(mapWithPluginDelegate(task, session, delegate ->
                delegate.transaction(schema, taskCount, runControl)
        ));
    }

    @Override
    public ConfigDiff resume(TaskSource taskSource, Schema schema, int taskCount, OutputPlugin.Control control) {
        final PluginTask task = taskSource.loadTask(PluginTask.class);
        final ExecSession session = Exec.session();
        final AsyncRunControl runControl = AsyncRunControl.start(task, control);
        return buildConfigDiff(mapWithPluginDelegate(task, session, delegate ->
                delegate.resume(schema, taskCount, runControl)
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
        return MultiTransactionalPageOutput.open(schema, taskIndex, mapWithPluginDelegate(task, session, Function.identity()));
    }

    private static ConfigDiff buildConfigDiff(List<OutputPluginDelegate.Transaction> transactions) {
        final ConfigDiff configDiff = Exec.newConfigDiff();
        Map<String, ConfigDiff> configDiffs = new HashMap<>();
        for (OutputPluginDelegate.Transaction transaction: transactions) {
            try {
                configDiffs.put(transaction.getTag(), transaction.getResult());
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
        for (int i = 0; i < task.getOutputConfigs().size(); i++) {
            final ConfigSource config = task.getOutputConfigs().get(i);
            final PluginType pluginType = config.get(PluginType.class, "type");
            final OutputPlugin outputPlugin = session.newPlugin(OutputPlugin.class, pluginType);

            final String tag = String.format("%s_%d", pluginType.getName(), i);

            // Merge ConfigDiff if exists
            if (task.getOutputConfigDiffs().isPresent()) {
                final ConfigDiff configDiff = task.getOutputConfigDiffs().get().get(tag);
                if (configDiff != null) {
                    config.merge(configDiff);
                } else {
                    LOGGER.debug("ConfigDiff for '{}' not found.", tag);
                }
            }
            // Set TaskSource if exists
            TaskSource taskSource = null;
            if (task.getTaskSources() != null) {
                taskSource = task.getTaskSources().get(tag);
            }

            result.add(action.apply(new OutputPluginDelegate(tag, outputPlugin, config, taskSource)));
        }
        return result;
    }
}
