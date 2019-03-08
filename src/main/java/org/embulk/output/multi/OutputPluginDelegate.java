package org.embulk.output.multi;

import com.google.common.util.concurrent.ThreadFactoryBuilder;
import org.embulk.config.ConfigDiff;
import org.embulk.config.ConfigSource;
import org.embulk.config.TaskReport;
import org.embulk.config.TaskSource;
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
    private final String tag;
    private final OutputPlugin plugin;
    private final ConfigSource config;
    private final TaskSource taskSource;
    private final ExecutorService executorService;

    OutputPluginDelegate(
            String tag,
            OutputPlugin plugin,
            ConfigSource config,
            TaskSource taskSource
    ) {
        this.tag = tag;
        this.plugin = plugin;
        this.config = config;
        this.taskSource = taskSource;
        this.executorService = Executors.newSingleThreadExecutor(
                new ThreadFactoryBuilder().setNameFormat(String.format(THREAD_NAME_FORMAT, tag)).build()
        );
    }

    Transaction transaction(Schema schema, int taskCount, AsyncRunControl runControl) {
        return new Transaction(executorService.submit(() -> {
            try {
                LOGGER.debug("Run #transaction for {}", getTag());
                return plugin.transaction(config, schema, taskCount, new Control(runControl));
            } catch (CancellationException e) {
                LOGGER.error("Canceled #transaction for {} by other plugin's error", getTag());
                throw e;
            } catch (Exception e) {
                LOGGER.error("Transaction for {} failed.", getTag(), e);
                runControl.cancel();
                throw e;
            } finally {
                executorService.shutdown();
            }
        }));
    }

    Transaction resume(Schema schema, int taskCount, AsyncRunControl runControl) {
        return new Transaction(executorService.submit(() -> {
            try {
                LOGGER.debug("Run #resume for {}", getTag());
                return plugin.resume(taskSource, schema, taskCount, new Control(runControl));
            } catch (CancellationException e) {
                LOGGER.error("Canceled #resume for {} by other plugin's error", getTag());
                throw e;
            } catch (Exception e) {
                LOGGER.error("Resume for {} failed.", getTag(), e);
                runControl.cancel();
                throw e;
            } finally {
                executorService.shutdown();
            }
        }));
    }

    void cleanup(Schema schema, int taskCount, List<TaskReport> successTaskReports) {
        LOGGER.debug("Run #cleanup for {}", getTag());
        List<TaskReport> successReportsForPlugin = new ArrayList<>();
        for (TaskReport successTaskReport : successTaskReports) {
            final TaskReport report = successTaskReport.get(TaskReports.class, MultiOutputPlugin.CONFIG_NAME_OUTPUT_TASK_REPORTS).get(tag);
            successReportsForPlugin.add(report);
        }
        plugin.cleanup(taskSource, schema, taskCount, successReportsForPlugin);
    }

    TransactionalPageOutput open(Schema schema, int taskIndex) {
        LOGGER.debug("Run #open for {}", getTag());
        return plugin.open(taskSource, schema, taskIndex);
    }

    String getTag() {
        return tag;
    }

    private class Control implements OutputPlugin.Control {
        private final AsyncRunControl runControl;

        Control(AsyncRunControl runControl) {
            this.runControl = runControl;
        }

        @Override
        public List<TaskReport> run(TaskSource taskSource) {
            runControl.addTaskSource(tag, taskSource);
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
                final TaskReport report = taskReport.get(TaskReports.class, MultiOutputPlugin.CONFIG_NAME_OUTPUT_TASK_REPORTS).get(tag);
                result.add(report);
            }
            return result;
        }
    }

    class Transaction {
        private final Future<ConfigDiff> future;

        private Transaction(Future<ConfigDiff> future) {
            this.future = future;
        }

        String getTag() {
            return OutputPluginDelegate.this.getTag();
        }

        ConfigDiff getResult() throws ExecutionException, InterruptedException {
            return future.get();
        }
    }
}
