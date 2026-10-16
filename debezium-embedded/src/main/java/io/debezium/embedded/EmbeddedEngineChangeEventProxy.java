/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.debezium.embedded;

import java.util.Collections;

import org.apache.kafka.connect.source.SourceRecord;

import io.debezium.engine.ChangeEvent;

/** Allows callers outside this package to create embedded engine change events. */
public class EmbeddedEngineChangeEventProxy {
    public static ChangeEvent<SourceRecord, SourceRecord> create(
                                                                 SourceRecord key, SourceRecord value, SourceRecord sourceRecord) {
        return new EmbeddedEngineChangeEvent<>(key, value, Collections.emptyList(), sourceRecord);
    }
}
