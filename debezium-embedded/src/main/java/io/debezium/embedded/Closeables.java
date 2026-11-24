/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.debezium.embedded;

import java.io.IOException;

/** Attempts every close and reports failures without skipping later resources. */
final class Closeables {
    private Closeables() {
    }

    static void closeAll(Iterable<? extends AutoCloseable> resources) throws IOException {
        IOException failure = null;
        for (AutoCloseable resource : resources) {
            try {
                resource.close();
            }
            catch (Exception | Error e) {
                if (failure == null) {
                    failure = new IOException("Failed to close resources", e);
                }
                else {
                    failure.addSuppressed(e);
                }
            }
        }
        if (failure != null) {
            throw failure;
        }
    }
}
