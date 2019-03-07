package org.embulk.output.multi;

import com.google.common.util.concurrent.ThreadFactoryBuilder;
import org.embulk.config.TaskReport;
import org.embulk.config.TaskSource;
import org.embulk.spi.OutputPlugin;

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.Callable;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.atomic.AtomicReferenceArray;

class RunControlTask implements Callable<List<TaskReport>> {
    private static final String THREAD_NAME_FORMAT = "multi-run-control-%d";
    private final MultiOutputPlugin.PluginTask task;
    private final OutputPlugin.Control control;
    private final CountDownLatch latch;
    private final AtomicReferenceArray<TaskSource> taskSources;
    private final ExecutorService executorService;
    private Future<List<TaskReport>> result;

    RunControlTask(MultiOutputPlugin.PluginTask task, OutputPlugin.Control control) {
        this.task = task;
        this.control = control;
        this.latch = new CountDownLatch(task.getOutputConfigs().size());
        this.taskSources = new AtomicReferenceArray<>(task.getOutputConfigs().size());
        this.executorService = Executors.newSingleThreadExecutor(
                new ThreadFactoryBuilder().setNameFormat(THREAD_NAME_FORMAT).build()
        );
        this.result = executorService.submit(this);
    }

    @Override
    public List<TaskReport> call() throws Exception {
        latch.await();
        List<TaskSource> taskSources = new ArrayList<>(this.taskSources.length());
        for (int i = 0; i < this.taskSources.length(); i++) {
            taskSources.add(this.taskSources.get(i));
        }
        task.setTaskSources(taskSources);
        return control.run(task.dump());
    }

    void cancel() {
        result.cancel(true);
    }

    void addTaskSource(int index, TaskSource taskSource) {
        taskSources.set(index, taskSource);
        latch.countDown();
    }

    List<TaskReport> waitAndGetResult() throws ExecutionException, InterruptedException {
        try {
            return result.get();
        } finally {
            executorService.shutdown();
        }
    }
}
