package org.embulk.output.multi;

import com.google.common.util.concurrent.ThreadFactoryBuilder;
import org.embulk.config.TaskReport;
import org.embulk.spi.Page;
import org.embulk.spi.Schema;
import org.embulk.spi.TransactionalPageOutput;

import java.util.concurrent.BlockingQueue;
import java.util.concurrent.Callable;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.function.Supplier;

class TransactionalPageOutputDelegate {
    private static final String THREAD_NAME_FORMAT = "multi-page-output-%s-%d";
    private final OutputPluginDelegate source;
    private final TransactionalPageOutput delegate;
    private final BlockingQueue<Supplier<Object>> taskQueue;
    private final ExecutorService executorService;
    private final Future<TaskReport> worker;
    private boolean isFailed;

    static TransactionalPageOutputDelegate open(Schema schema, int taskIndex, OutputPluginDelegate delegate) {
        return new TransactionalPageOutputDelegate(taskIndex, delegate, delegate.open(schema, taskIndex));
    }

    private TransactionalPageOutputDelegate(
            int taskIndex,
            OutputPluginDelegate source,
            TransactionalPageOutput delegate
    ) {
        this.source = source;
        this.delegate = delegate;
        this.taskQueue = new LinkedBlockingQueue<>();
        this.executorService = Executors.newSingleThreadExecutor(
                new ThreadFactoryBuilder().setNameFormat(String.format(THREAD_NAME_FORMAT, source.getTag(), taskIndex)).build()
        );
        this.worker = executorService.submit(new Worker());
        this.isFailed = false;
    }

    void add(Page page) {
        taskQueue.add(() -> {
            delegate.add(page);
            return null;
        });
    }

    void finish() {
        taskQueue.add(() -> {
            delegate.finish();
            return null;
        });
    }

    void close() {
        taskQueue.add(() -> {
            delegate.close();
            return null;
        });
    }

    void abort() {
        // Run abort only if the output failed.
        if (isFailed) {
            delegate.abort();
        }
    }

    TaskReport commit() {
        taskQueue.add(delegate::commit);
        try {
            return worker.get();
        } catch (InterruptedException e) {
            isFailed = true;
            Thread.currentThread().interrupt();
            throw new RuntimeException(e);
        } catch (ExecutionException e) {
            isFailed = true;
            throw new PluginExecutionException(source, e.getCause());
        } finally {
            executorService.shutdown();
        }
    }

    String getTag() {
        return source.getTag();
    }

    private class Worker implements Callable<TaskReport> {
        @Override
        public TaskReport call() throws InterruptedException {
            while (true) {
                final Object result = taskQueue.take().get();
                if (result instanceof TaskReport) {
                    return (TaskReport) result;
                }
            }
        }
    }
}
