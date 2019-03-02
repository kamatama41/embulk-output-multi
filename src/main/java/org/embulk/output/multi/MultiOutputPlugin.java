package org.embulk.output.multi;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;
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
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Optional;
import java.util.concurrent.Callable;
import java.util.concurrent.CancellationException;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.function.Function;

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
    private static final String CONFIG_NAME_OUTPUT_TASK_REPORTS = "output_task_reports";
    private static final Logger LOGGER = LoggerFactory.getLogger(MultiOutputPlugin.class);
    private final ExecutorService executorService;

    public MultiOutputPlugin() {
        this.executorService = Executors.newCachedThreadPool();
    }

    @Override
    public ConfigDiff transaction(ConfigSource config, Schema schema, int taskCount, OutputPlugin.Control control) {
        final PluginTask task = config.loadConfig(PluginTask.class);
        if (task.getOutputConfigs().isEmpty()) {
            throw new ConfigException("'outputs' must have more than than or equals to 1 element.");
        }
        final ExecSession session = Exec.session();
        final RunControlTask controlTask = new RunControlTask(task, control, executorService);
        controlTask.runAsynchronously();
        return buildConfigDiff(mapWithPluginDelegate(task, session, delegate ->
                delegate.transaction(schema, taskCount, controlTask)
        ));
    }

    @Override
    public ConfigDiff resume(TaskSource taskSource, Schema schema, int taskCount, OutputPlugin.Control control) {
        final PluginTask task = taskSource.loadTask(PluginTask.class);
        final ExecSession session = Exec.session();
        final RunControlTask controlTask = new RunControlTask(task, control, executorService);
        controlTask.runAsynchronously();
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
        final List<TransactionalPageOutput> delegates = mapWithPluginDelegate(task, session, delegate ->
                delegate.open(schema, taskIndex)
        );

        return new TransactionalPageOutput() {
            @Override
            public void add(Page original) {
                final Buffer originalBuffer = original.buffer();
                for (TransactionalPageOutput output : delegates) {
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
                for (TransactionalPageOutput output : delegates) {
                    output.finish();
                }
            }

            @Override
            public void close() {
                for (TransactionalPageOutput output : delegates) {
                    output.close();
                }
            }

            @Override
            public void abort() {
                for (TransactionalPageOutput output : delegates) {
                    output.abort();
                }
            }

            @Override
            public TaskReport commit() {
                final TaskReport report = Exec.newTaskReport();
                final List<TaskReport> reports = new ArrayList<>();
                for (TransactionalPageOutput output : delegates) {
                    reports.add(output.commit());
                }
                report.set(CONFIG_NAME_OUTPUT_TASK_REPORTS, new TaskReports(reports));
                return report;
            }
        };
    }

    private static class RunControlTask implements Callable<List<TaskReport>> {
        private final PluginTask task;
        private final OutputPlugin.Control control;
        private final ExecutorService executorService;
        private final CountDownLatch latch;
        private final TaskSource[] taskSources;
        private Future<List<TaskReport>> future;

        RunControlTask(PluginTask task, OutputPlugin.Control control, ExecutorService executorService) {
            this.task = task;
            this.control = control;
            this.executorService = executorService;
            this.latch = new CountDownLatch(task.getOutputConfigs().size());
            this.taskSources = new TaskSource[task.getOutputConfigs().size()];
        }

        @Override
        public List<TaskReport> call() throws Exception {
            latch.await();
            task.setTaskSources(Arrays.asList(taskSources));
            return control.run(task.dump());
        }

        void runAsynchronously() {
            future = executorService.submit(this);
        }

        void cancel() {
            future.cancel(true);
        }

        void addTaskSource(int index, TaskSource taskSource) {
            taskSources[index] = taskSource;
            latch.countDown();
        }

        List<TaskReport> waitAndGetResult() throws ExecutionException, InterruptedException {
            return future.get();
        }
    }

    private static class OutputPluginDelegate {
        private final int pluginIndex;
        private final PluginType type;
        private final OutputPlugin plugin;
        private final ConfigSource config;
        private final TaskSource taskSource;
        private final ExecutorService executorService;

        OutputPluginDelegate(
                int pluginIndex,
                PluginType type,
                OutputPlugin plugin,
                ConfigSource config,
                TaskSource taskSource,
                ExecutorService executorService
        ) {
            this.pluginIndex = pluginIndex;
            this.type = type;
            this.plugin = plugin;
            this.config = config;
            this.taskSource = taskSource;
            this.executorService = executorService;
        }

        Future<ConfigDiff> transaction(Schema schema, int taskCount, RunControlTask controlTask) {
            LOGGER.debug("Run transaction for {}", getPluginNameForLogging());
            return executorService.submit(() -> {
                try {
                    return plugin.transaction(config, schema, taskCount, new ControlDelegate(pluginIndex, controlTask));
                } catch (CancellationException e) {
                    LOGGER.error("Canceled transaction for {} by other plugin's error", getPluginNameForLogging());
                    throw e;
                } catch (Exception e) {
                    LOGGER.error("Transaction for {} failed.", getPluginNameForLogging(), e);
                    controlTask.cancel();
                    throw e;
                }
            });
        }

        Future<ConfigDiff> resume(Schema schema, int taskCount, RunControlTask controlTask) {
            LOGGER.debug("Run resume for {}", getPluginNameForLogging());
            return executorService.submit(() -> {
                try {
                    return plugin.resume(taskSource, schema, taskCount, new ControlDelegate(pluginIndex, controlTask));
                } catch (CancellationException e) {
                    LOGGER.error("Canceled resume for {} by other plugin's error", getPluginNameForLogging());
                    throw e;
                } catch (Exception e) {
                    LOGGER.error("Resume for {} failed.", getPluginNameForLogging(), e);
                    controlTask.cancel();
                    throw e;
                }
            });
        }

        void cleanup(Schema schema, int taskCount, List<TaskReport> successTaskReports) {
            LOGGER.debug("Run cleanup for {}", getPluginNameForLogging());
            List<TaskReport> successReportsForPlugin = new ArrayList<>();
            for (TaskReport successTaskReport : successTaskReports) {
                final TaskReport report = successTaskReport.get(TaskReports.class, CONFIG_NAME_OUTPUT_TASK_REPORTS).get(pluginIndex);
                successReportsForPlugin.add(report);
                plugin.cleanup(taskSource, schema, taskCount, successReportsForPlugin);
            }
        }

        TransactionalPageOutput open(Schema schema, int taskIndex) {
            LOGGER.debug("Run open for {}", getPluginNameForLogging());
            return plugin.open(taskSource, schema, taskIndex);
        }

        private String getPluginNameForLogging() {
            return String.format("%s output plugin (pluginIndex: %s)", type.getName(), pluginIndex);
        }
    }

    private static class ControlDelegate implements OutputPlugin.Control {
        private final int pluginIndex;
        private final RunControlTask controlTask;

        ControlDelegate(int index, RunControlTask controlTask) {
            this.pluginIndex = index;
            this.controlTask = controlTask;
        }

        @Override
        public List<TaskReport> run(TaskSource taskSource) {
            controlTask.addTaskSource(pluginIndex, taskSource);
            List<TaskReport> reports;
            try {
                reports = controlTask.waitAndGetResult();
            } catch (InterruptedException | ExecutionException e) {
                throw new RuntimeException(e);
            }

            final List<TaskReport> result = new ArrayList<>();
            for (TaskReport taskReport : reports) {
                final TaskReport report = taskReport.get(TaskReports.class, CONFIG_NAME_OUTPUT_TASK_REPORTS).get(pluginIndex);
                result.add(report);
            }
            return result;
        }
    }

    private static class TaskReports {
        private final List<TaskReport> reports;

        @JsonCreator
        TaskReports(@JsonProperty("reports") List<TaskReport> reports) {
            this.reports = reports;
        }

        @JsonProperty("reports")
        List<TaskReport> getReports() {
            return reports;
        }

        TaskReport get(int index) {
            return reports.get(index);
        }
    }

    private static ConfigDiff buildConfigDiff(List<Future<ConfigDiff>> runPluginTasks) {
        final ConfigDiff configDiff = Exec.newConfigDiff();
        List<ConfigDiff> configDiffs = new ArrayList<>();
        for (Future<ConfigDiff> pluginTask : runPluginTasks) {
            try {
                configDiffs.add(pluginTask.get());
            } catch (InterruptedException | ExecutionException e) {
                throw new RuntimeException(e);
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
            result.add(action.apply(new OutputPluginDelegate(pluginIndex, pluginType, outputPlugin, config, taskSource, executorService)));
        }
        return result;
    }
}
