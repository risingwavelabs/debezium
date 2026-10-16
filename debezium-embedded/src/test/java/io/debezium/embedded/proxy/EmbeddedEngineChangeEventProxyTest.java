/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.debezium.embedded.proxy;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertSame;
import static org.junit.Assert.assertTrue;

import java.util.Map;

import org.apache.kafka.connect.data.Schema;
import org.apache.kafka.connect.source.SourceRecord;
import org.junit.Test;

import io.debezium.embedded.EmbeddedEngineChangeEvent;
import io.debezium.embedded.EmbeddedEngineChangeEventProxy;
import io.debezium.engine.ChangeEvent;

public class EmbeddedEngineChangeEventProxyTest {

    @Test
    public void shouldPreservePayloadAndSourceRecordForOffsetCommit() {
        SourceRecord key = record("key", 1L);
        SourceRecord value = record("value", 2L);
        SourceRecord source = record("source", 3L);

        ChangeEvent<SourceRecord, SourceRecord> event = EmbeddedEngineChangeEventProxy.create(key, value, source);

        assertSame(key, event.key());
        assertSame(value, event.value());
        assertTrue(event.headers().isEmpty());
        assertEquals(source.topic(), event.destination());
        assertEquals(source.kafkaPartition(), event.partition());
        EmbeddedEngineChangeEvent<?, ?, ?> embedded = (EmbeddedEngineChangeEvent<?, ?, ?>) event;
        assertSame(source, embedded.sourceRecord());
        assertSame(value, embedded.record());
        assertEquals(Map.of("position", 3L), embedded.sourceRecord().sourceOffset());
    }

    @Test
    public void shouldAcceptNullKeyForCheckpointRecord() {
        SourceRecord source = record("checkpoint", 42L);

        ChangeEvent<SourceRecord, SourceRecord> event = EmbeddedEngineChangeEventProxy.create(null, source, source);

        assertNull(event.key());
        assertSame(source, event.value());
        assertSame(source, ((EmbeddedEngineChangeEvent<?, ?, ?>) event).sourceRecord());
    }

    private SourceRecord record(String topic, long offset) {
        return new SourceRecord(Map.of("server", "test"), Map.of("position", offset), topic, 0,
                Schema.STRING_SCHEMA, "payload");
    }
}
