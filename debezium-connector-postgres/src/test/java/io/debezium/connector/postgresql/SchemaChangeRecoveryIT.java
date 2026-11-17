/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.debezium.connector.postgresql;

import static org.assertj.core.api.Assertions.assertThat;

import java.util.List;

import org.apache.kafka.connect.data.Struct;
import org.apache.kafka.connect.source.SourceRecord;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import io.debezium.config.Configuration;
import io.debezium.data.Envelope;
import io.debezium.embedded.async.AbstractAsyncEngineConnectorTest;
import io.debezium.junit.logging.LogInterceptor;

/** Verifies that the WAL-position search does not publish schema changes before normal replay. */
public class SchemaChangeRecoveryIT extends AbstractAsyncEngineConnectorTest {
    private static final String TABLE = "public.schema_change_recovery";
    private static final String TOPIC = TestHelper.TEST_SERVER + "." + TABLE;

    @Before
    public void before() throws Exception {
        initializeConnectorTestFramework();
        TestHelper.dropDefaultReplicationSlot();
        TestHelper.dropPublication();
        TestHelper.execute("DROP TABLE IF EXISTS " + TABLE + ";",
                "CREATE TABLE " + TABLE + " (id INTEGER PRIMARY KEY, payload VARCHAR NOT NULL)");
    }

    @After
    public void after() throws Exception {
        stopConnector();
        TestHelper.dropDefaultReplicationSlot();
        TestHelper.dropPublication();
        TestHelper.execute("DROP TABLE IF EXISTS " + TABLE);
    }

    @Test
    public void shouldDispatchRelationOnlyOnceOnRecovery() throws Exception {
        assertRecoverySchemaChange(false);
    }

    @Test
    public void shouldPreserveSchemaChangeMadeWhileOffline() throws Exception {
        assertRecoverySchemaChange(true);
    }

    private void assertRecoverySchemaChange(boolean changeSchema) throws Exception {
        Configuration config = TestHelper.defaultConfig()
                .with(PostgresConnectorConfig.PLUGIN_NAME, "pgoutput")
                .with(PostgresConnectorConfig.SNAPSHOT_MODE, "no_data")
                .with(PostgresConnectorConfig.TABLE_INCLUDE_LIST, TABLE)
                .with(PostgresConnectorConfig.DROP_SLOT_ON_STOP, false)
                .with(PostgresConnectorConfig.INCLUDE_SCHEMA_CHANGES, true)
                .build();
        start(PostgresConnector.class, config);
        waitForStreamingRunning("postgres", TestHelper.TEST_SERVER);
        TestHelper.execute("INSERT INTO " + TABLE + " VALUES (1, 'before')");
        SourceRecords initial = consumeRecordsByTopicUntil((count, record) -> TOPIC.equals(record.topic()));
        assertThat(initial.recordsForTopic(TOPIC)).hasSize(1);
        stopConnector();

        if (changeSchema) {
            TestHelper.execute("ALTER TABLE " + TABLE + " ADD COLUMN added VARCHAR DEFAULT 'offline'");
        }
        TestHelper.execute("INSERT INTO " + TABLE + " (id, payload) VALUES (2, 'after')");

        LogInterceptor recoveryLog = new LogInterceptor(PostgresStreamingChangeEventSource.class);
        start(PostgresConnector.class, config);
        SourceRecords recovered = consumeRecordsByTopicUntil((count, record) -> TOPIC.equals(record.topic())
                && ((Struct) record.value()).getStruct(Envelope.FieldName.AFTER).getInt32("id") == 2);

        assertThat(recoveryLog.containsMessage("Searching for WAL resume position")).isTrue();
        // Normal replay can describe the old schema and then the offline DDL's new schema.
        // Neither descriptor should also be published by the WAL-position search.
        var schemaChanges = recovered.recordsForTopic(TestHelper.TEST_SERVER);
        assertThat(schemaChanges).extracting(SchemaChangeRecoveryIT::columnNames).doesNotHaveDuplicates();
        var rows = recovered.recordsForTopic(TOPIC);
        Struct after = ((Struct) rows.get(rows.size() - 1).value()).getStruct(Envelope.FieldName.AFTER);
        assertThat(after.getInt32("id")).isEqualTo(2);
        if (changeSchema) {
            assertThat(after.getString("added")).isEqualTo("offline");
            assertThat(schemaChanges).filteredOn(record -> columnNames(record).contains("added")).hasSize(1);
        }
        else {
            assertThat(schemaChanges).hasSize(1);
        }
    }

    private static List<String> columnNames(SourceRecord record) {
        List<Struct> changes = ((Struct) record.value()).getArray("tableChanges");
        List<Struct> columns = changes.get(0).getStruct("table").getArray("columns");
        return columns.stream().map(column -> column.getString("name")).toList();
    }
}
