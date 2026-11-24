/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.debezium.embedded.async;

import java.util.Map;

import org.apache.kafka.connect.source.SourceTask;

/** Orders one task's only cleanup attempt after its startup has finished. */
final class TaskCleanup {
    final SourceTask task;
    private final LifecycleGate startup = new LifecycleGate();

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

    void stop(Runnable beforeStop) {
        boolean interrupted = startup.awaitCompletion();
        try {
            try {
                beforeStop.run();
            }
            finally {
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
