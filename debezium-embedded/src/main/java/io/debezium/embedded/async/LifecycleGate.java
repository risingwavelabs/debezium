/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.debezium.embedded.async;

import java.util.concurrent.CountDownLatch;

/** Orders resource creation before cleanup, including operations that ignore interruption. */
final class LifecycleGate {
    private final CountDownLatch finished = new CountDownLatch(1);
    private boolean claimed;

    synchronized boolean begin() {
        if (claimed) {
            return false;
        }
        claimed = true;
        return true;
    }

    void finish() {
        finished.countDown();
    }

    /** Cancels an operation that has not begun, otherwise waits for its actual completion. */
    boolean awaitCompletion() {
        if (begin()) {
            finish();
        }
        boolean interrupted = false;
        while (true) {
            try {
                finished.await();
                return interrupted;
            }
            catch (InterruptedException e) {
                interrupted = true;
            }
        }
    }
}
