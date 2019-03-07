package org.embulk.output.multi;

import com.google.common.util.concurrent.ThreadFactoryBuilder;
import org.embulk.config.ConfigDiff;
import org.embulk.config.ConfigSource;
import org.embulk.config.TaskReport;
import org.embulk.config.TaskSource;
import org.embulk.plugin.PluginType;
import org.embulk.spi.OutputPlugin;
import org.embulk.spi.Schema;
import org.embulk.spi.TransactionalPageOutput;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.CancellationException;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;

class OutputPluginDelegate {
    private static final Logger LOGGER = LoggerFactory.getLogger(OutputPluginDelegate.class);
    private static final String THREAD_NAME_FORMAT = "multi-output-plugin-%s-%%d";
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
            TaskSource taskSource
    ) {
        this.pluginIndex = pluginIndex;
        this.type = type;
        this.plugin = plugin;
        this.config = config;
        this.taskSource = taskSource;
        this.executorService = Executors.newSingleThreadExecutor(
                new ThreadFactoryBuilder().setNameFormat(String.format(THREAD_NAME_FORMAT, generatePluginCode(type, pluginIndex))).build()
        );
    }

    Future<ConfigDiff> transaction(Schema schema, int taskCount, AsyncRunControl runControl) {
        return executorService.submit(() -> {
            try {
                LOGGER.debug("Run #transaction for {}", getPluginCode());
                return plugin.transaction(config, schema, taskCount, new Control(pluginIndex, runControl));
            } catch (CancellationException e) {
                LOGGER.error("Canceled #transaction for {} by other plugin's error", getPluginCode());
                throw e;
            } catch (Exception e) {
                LOGGER.error("Transaction for {} failed.", getPluginCode(), e);
                runControl.cancel();
                throw e;
            } finally {
                executorService.shutdown();
            }
        });
    }

    Future<ConfigDiff> resume(Schema schema, int taskCount, AsyncRunControl runControl) {
        return executorService.submit(() -> {
            try {
                LOGGER.debug("Run #resume for {}", getPluginCode());
                return plugin.resume(taskSource, schema, taskCount, new Control(pluginIndex, runControl));
            } catch (CancellationException e) {
                LOGGER.error("Canceled #resume for {} by other plugin's error", getPluginCode());
                throw e;
            } catch (Exception e) {
                LOGGER.error("Resume for {} failed.", getPluginCode(), e);
                runControl.cancel();
                throw e;
            } finally {
                executorService.shutdown();
            }
        });
    }

    void cleanup(Schema schema, int taskCount, List<TaskReport> successTaskReports) {
        LOGGER.debug("Run #cleanup for {}", getPluginCode());
        List<TaskReport> successReportsForPlugin = new ArrayList<>();
        for (TaskReport successTaskReport : successTaskReports) {
            final TaskReport report = successTaskReport.get(TaskReports.class, MultiOutputPlugin.CONFIG_NAME_OUTPUT_TASK_REPORTS).get(pluginIndex);
            successReportsForPlugin.add(report);
        }
        plugin.cleanup(taskSource, schema, taskCount, successReportsForPlugin);
    }

    TransactionalPageOutput open(Schema schema, int taskIndex) {
        LOGGER.debug("Run #open for {}", getPluginCode());
        return plugin.open(taskSource, schema, taskIndex);
    }

    PluginType getType() {
        return type;
    }

    String getPluginCode() {
        return generatePluginCode(type, pluginIndex);
    }

    private static String generatePluginCode(PluginType type, int pluginIndex) {
        return String.format("%s-%d", type.getName(), pluginIndex);
    }

    private static class Control implements OutputPlugin.Control {
        private final int pluginIndex;
        private final AsyncRunControl runControl;

        Control(int index, AsyncRunControl runControl) {
            this.pluginIndex = index;
            this.runControl = runControl;
        }

        @Override
        public List<TaskReport> run(TaskSource taskSource) {
            runControl.addTaskSource(pluginIndex, taskSource);
            List<TaskReport> reports;
            try {
                reports = runControl.waitAndGetResult();
            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
                throw new RuntimeException(e);
            } catch (ExecutionException e) {
                throw new RuntimeException(e.getCause());
            }

            final List<TaskReport> result = new ArrayList<>();
            for (TaskReport taskReport : reports) {
                final TaskReport report = taskReport.get(TaskReports.class, MultiOutputPlugin.CONFIG_NAME_OUTPUT_TASK_REPORTS).get(pluginIndex);
                result.add(report);
            }
            return result;
        }
    }
}
