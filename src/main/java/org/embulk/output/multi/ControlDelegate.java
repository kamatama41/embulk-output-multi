package org.embulk.output.multi;

import org.embulk.config.TaskReport;
import org.embulk.config.TaskSource;
import org.embulk.spi.OutputPlugin;

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.ExecutionException;

class ControlDelegate implements OutputPlugin.Control {
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
