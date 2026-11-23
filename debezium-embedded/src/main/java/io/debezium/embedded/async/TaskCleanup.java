/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.debezium.embedded.async;

import java.util.Map;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Future;

import org.apache.kafka.connect.source.SourceTask;

/** Owns one task's startup and cleanup attempts; a cancelled wait never counts as task completion. */
final class TaskCleanup {
    final SourceTask task;
    private final LifecycleGate startup = new LifecycleGate();
    private volatile boolean stopped;
    private volatile Throwable failure;
    private volatile Future<?> attempt;

    TaskCleanup(SourceTask task) {
        this.task = task;
    }

    void start(Map<String, String> config) {
        if (startup.begin()) {
            try {
                task.start(config);
            }
            finally {
                startup.finish();
            }
        }
    }

    Throwable failure() {
        return failure;
    }

    boolean pending() {
        Future<?> current = attempt;
        return !stopped || (current != null && !current.isDone());
    }

    /** Never race the owner's first attempt or skip its final commit. */
    synchronized Future<?> retry(ExecutorService executor) {
        return attempt == null ? null : submit(executor, () -> {
        });
    }

    /** Initial cleanup includes a final commit; retries only repeat the failed task stop. */
    synchronized Future<?> submit(ExecutorService executor, Runnable beforeStop) {
        if (!stopped && (attempt == null || attempt.isDone())) {
            attempt = executor.submit(() -> {
                boolean interrupted = startup.awaitCompletion();
                try {
                    try {
                        beforeStop.run();
                    }
                    finally {
                        try {
                            task.stop();
                            failure = null;
                            stopped = true;
                        }
                        catch (RuntimeException | Error e) {
                            failure = e;
                            throw e;
                        }
                    }
                }
                finally {
                    if (interrupted) {
                        Thread.currentThread().interrupt();
                    }
                }
            });
        }
        return attempt;
    }
}
