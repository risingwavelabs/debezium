/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.debezium.openlineage;

import static io.debezium.openlineage.OpenLineageConfig.OPEN_LINEAGE_INTEGRATION_ENABLED;
import static org.junit.Assert.assertThrows;

import java.util.List;
import java.util.Map;

import org.junit.Test;

import io.debezium.connector.common.DebeziumTaskState;

public class DebeziumOpenLineageEmitterTest {

    @Test
    public void shouldEmitWithoutInitializationWhenDisabled() {
        emitAll(ConnectorContext.from(Map.of(OPEN_LINEAGE_INTEGRATION_ENABLED, "false"), "postgres"));
    }

    @Test
    public void shouldEmitWithoutInitializationWhenFlagIsAbsent() {
        emitAll(ConnectorContext.from(Map.of(), "postgres"));
    }

    @Test
    public void shouldRemainNoOpAfterCleanupWhenDisabled() {
        Map<String, String> config = Map.of(OPEN_LINEAGE_INTEGRATION_ENABLED, "false", "topic.prefix", "disabled");
        ConnectorContext context = ConnectorContext.from(config, "postgres");
        DebeziumOpenLineageEmitter.init(config, "postgres");
        DebeziumOpenLineageEmitter.cleanup(context);
        emitAll(context);
    }

    @Test
    public void shouldRequireInitializationWhenEnabled() {
        ConnectorContext context = ConnectorContext.from(
                Map.of(OPEN_LINEAGE_INTEGRATION_ENABLED, "true", "topic.prefix", "uninitialized"), "postgres");
        assertThrows(IllegalStateException.class, () -> DebeziumOpenLineageEmitter.emit(context, DebeziumTaskState.RUNNING));
    }

    @Test
    public void shouldPreserveEnabledEmitterLifecycle() {
        Map<String, String> config = Map.of(OPEN_LINEAGE_INTEGRATION_ENABLED, "true", "topic.prefix", "enabled");
        ConnectorContext context = ConnectorContext.from(config, "postgres");
        try {
            DebeziumOpenLineageEmitter.init(config, "postgres");
            emitAll(context);
        }
        finally {
            DebeziumOpenLineageEmitter.cleanup(context);
        }
        assertThrows(IllegalStateException.class, () -> DebeziumOpenLineageEmitter.emit(context, DebeziumTaskState.RUNNING));
    }

    private void emitAll(ConnectorContext context) {
        RuntimeException failure = new RuntimeException("test failure");
        DebeziumOpenLineageEmitter.emit(context, DebeziumTaskState.RUNNING);
        DebeziumOpenLineageEmitter.emit(context, DebeziumTaskState.RUNNING, failure);
        DebeziumOpenLineageEmitter.emit(context, DebeziumTaskState.RUNNING, List.of());
        DebeziumOpenLineageEmitter.emit(context, DebeziumTaskState.RUNNING, List.of(), failure);
    }
}
