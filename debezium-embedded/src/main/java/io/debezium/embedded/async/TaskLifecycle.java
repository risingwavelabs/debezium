/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.debezium.embedded.async;

import java.util.Map;

import org.apache.kafka.connect.source.SourceRecord;
import org.apache.kafka.connect.source.SourceTask;

/**
 * Orders startup and task commit callbacks before the single stop attempt. Once stopping begins,
 * consumers may still acknowledge offsets, but they must not call back into the connector task.
 */
final class TaskLifecycle {
    final SourceTask task;
    private final LifecycleGate startup = new LifecycleGate();
    private final Object callbackLock = new Object();
    private boolean stopping;

    TaskLifecycle(SourceTask task) {
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

    void acknowledge(SourceRecord record, Runnable recordOffset) throws InterruptedException {
        synchronized (callbackLock) {
            if (!stopping) {
                task.commitRecord(record, null);
            }
            recordOffset.run();
        }
    }

    @FunctionalInterface
    interface CommitAction {
        void run() throws InterruptedException;
    }

    void commitBatch(CommitAction commit) throws InterruptedException {
        synchronized (callbackLock) {
            if (!stopping) {
                commit.run();
            }
        }
    }

    void stop(Runnable beforeStop) {
        boolean interrupted = startup.awaitCompletion();
        try {
            try {
                synchronized (callbackLock) {
                    // Close admission and finish the pre-stop commit under the same lock as
                    // consumer callbacks, so no callback can cross the stop boundary.
                    stopping = true;
                    // Monitor acquisition is uninterruptible. Preserve an interrupt received
                    // while awaiting a callback, but still attempt the final commit and stop.
                    interrupted |= Thread.interrupted();
                    beforeStop.run();
                }
            }
            finally {
                // A task stop may release a consumer: never hold callbackLock across it.
                task.stop();
            }
        }
        finally {
            if (interrupted) {
                Thread.currentThread().interrupt();
            }
        }
    }
}
