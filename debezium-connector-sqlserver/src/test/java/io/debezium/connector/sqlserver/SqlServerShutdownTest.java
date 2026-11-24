/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.debezium.connector.sqlserver;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertThrows;
import static org.junit.Assert.assertTrue;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.atLeastOnce;
import static org.mockito.Mockito.doAnswer;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import java.lang.reflect.Field;
import java.sql.Connection;
import java.sql.SQLException;
import java.time.Duration;
import java.util.HashMap;
import java.util.Set;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicReference;

import org.apache.kafka.connect.source.SourceTaskContext;
import org.junit.Test;

import io.debezium.config.CommonConnectorConfig;
import io.debezium.config.Configuration;
import io.debezium.jdbc.JdbcConnection;
import io.debezium.pipeline.EventDispatcher;
import io.debezium.pipeline.source.spi.StreamingChangeEventSource;
import io.debezium.pipeline.spi.Offsets;

public class SqlServerShutdownTest {
    @Test(timeout = 10000)
    public void stopsBeforeStreamingWithBlockedJdbcMonitor() throws Exception {
        stopBlockedTask(false);
    }

    @Test(timeout = 10000)
    public void interruptedStreamingStopStillClosesBothConnections() throws Exception {
        stopBlockedTask(true);
    }

    @Test(timeout = 10000)
    public void connectionPublishedAfterStopIsAbortedBeforeInitialization() throws Exception {
        SqlServerConnectorConfig config = new SqlServerConnectorConfig(Configuration.create()
                .with("topic.prefix", "shutdown-test")
                .with("database.names", "test")
                .with("database.hostname", "localhost")
                .with("database.initial.statements", "SELECT 1")
                .build());
        SqlServerConnection connection = new SqlServerConnection(config, null, Set.of(), true);
        JdbcConnection.ConnectionFactory factory = mock(JdbcConnection.ConnectionFactory.class);
        Connection raw = mock(Connection.class);
        CountDownLatch connecting = new CountDownLatch(1);
        CountDownLatch publish = new CountDownLatch(1);
        when(factory.connect(any())).thenAnswer(invocation -> {
            connecting.countDown();
            assertTrue(publish.await(2, TimeUnit.SECONDS));
            return raw;
        });
        setField(JdbcConnection.class, connection, "factory", factory);
        var executor = Executors.newSingleThreadExecutor();
        try {
            var open = executor.submit(() -> connection.connection());
            try {
                assertTrue(connecting.await(2, TimeUnit.SECONDS));
                connection.forceClose();
                publish.countDown();
                ExecutionException failure = assertThrows(ExecutionException.class, () -> open.get(2, TimeUnit.SECONDS));
                assertTrue(failure.getCause() instanceof SQLException);
                verify(raw, atLeastOnce()).abort(any());
                verify(raw, never()).createStatement();
            }
            finally {
                publish.countDown();
            }
        }
        finally {
            executor.shutdownNow();
        }
    }

    @Test(timeout = 10000)
    public void failedAbortIsReportedAndPreventsReopening() throws Exception {
        SqlServerConnectorConfig config = new SqlServerConnectorConfig(Configuration.create()
                .with("topic.prefix", "shutdown-test")
                .with("database.names", "test")
                .with("database.hostname", "localhost").build());
        SqlServerConnection connection = new SqlServerConnection(config, null, Set.of(), true);
        Connection raw = mock(Connection.class);
        setField(JdbcConnection.class, connection, "conn", raw);
        org.mockito.Mockito.doThrow(new SQLException("abort failed")).when(raw).abort(any());
        assertThrows(org.apache.kafka.connect.errors.ConnectException.class, connection::forceClose);
        verify(raw).abort(any());
        assertThrows(SQLException.class, connection::connection);
    }

    private void stopBlockedTask(boolean interrupt) throws Exception {
        SqlServerConnectorConfig config = new SqlServerConnectorConfig(Configuration.create()
                .with("topic.prefix", "shutdown-test")
                .with("database.names", "test")
                .with("database.hostname", "localhost")
                .with(CommonConnectorConfig.EXECUTOR_SHUTDOWN_TIMEOUT_MS, interrupt ? 30000 : 10)
                .build());
        SqlServerConnection data = new SqlServerConnection(config, null, Set.of(), true);
        SqlServerConnection metadata = new SqlServerConnection(config, null, Set.of(), true);
        Connection rawData = mock(Connection.class);
        Connection rawMetadata = mock(Connection.class);
        setField(JdbcConnection.class, data, "conn", rawData);
        setField(JdbcConnection.class, metadata, "conn", rawMetadata);
        CountDownLatch entered = new CountDownLatch(1);
        CountDownLatch release = new CountDownLatch(1);
        doAnswer(invocation -> {
            release.countDown();
            return null;
        }).when(rawData).abort(any());

        class Coordinator extends SqlServerChangeEventSourceCoordinator {
            Coordinator() {
                super(Offsets.of(new HashMap<>()), null, SqlServerConnector.class, config, null, null,
                        mock(EventDispatcher.class), null, io.debezium.util.Clock.system(), null, null, null,
                        () -> {
                            data.forceClose();
                            metadata.forceClose();
                        });
                if (interrupt) {
                    streamingSource = mock(StreamingChangeEventSource.class);
                }
            }

            void block() {
                executor.submit(() -> {
                    synchronized (data) {
                        entered.countDown();
                        // Model JDBC I/O which ignores interrupts while holding its monitor.
                        boolean done = false;
                        while (!done) {
                            try {
                                release.await();
                                done = true;
                            }
                            catch (InterruptedException ignored) {
                            }
                        }
                    }
                });
            }

            boolean terminated() throws InterruptedException {
                return executor.awaitTermination(2, TimeUnit.SECONDS);
            }
        }
        Coordinator testCoordinator = new Coordinator();
        SqlServerConnectorTask task = new SqlServerConnectorTask() {
            @Override
            public io.debezium.pipeline.ChangeEventSourceCoordinator<SqlServerPartition, SqlServerOffsetContext> start(Configuration ignored) {
                return testCoordinator;
            }

            @Override
            protected Iterable<io.debezium.config.Field> getAllConfigurationFields() {
                return java.util.List.of();
            }
        };
        task.initialize(mock(SourceTaskContext.class));
        task.start(new HashMap<String, String>());
        setField(SqlServerConnectorTask.class, task, "dataConnection", data);
        setField(SqlServerConnectorTask.class, task, "metadataConnection", metadata);
        testCoordinator.block();
        assertTrue(entered.await(2, TimeUnit.SECONDS));
        AtomicReference<Throwable> failure = new AtomicReference<>();
        Thread stopper = new Thread(() -> {
            try {
                task.stop();
            }
            catch (Throwable e) {
                failure.set(e);
            }
        });
        stopper.setDaemon(true);
        try {
            stopper.start();
            if (interrupt) {
                long deadline = System.nanoTime() + Duration.ofSeconds(2).toNanos();
                while (stopper.getState() != Thread.State.TIMED_WAITING && System.nanoTime() < deadline) {
                    Thread.yield();
                }
                assertEquals(Thread.State.TIMED_WAITING, stopper.getState());
                stopper.interrupt();
            }
            stopper.join(3000);
            assertFalse("stop waited for the JDBC monitor", stopper.isAlive());
            assertTrue(testCoordinator.terminated());
            verify(rawData, atLeastOnce()).abort(any());
            verify(rawMetadata, atLeastOnce()).abort(any());
            assertThrows(SQLException.class, data::connection);
            assertThrows(SQLException.class, metadata::connection);
            if (interrupt) {
                assertTrue(failure.get().getCause() instanceof InterruptedException);
            }
            else {
                assertNull(failure.get());
            }
        }
        finally {
            release.countDown();
            stopper.join(2000);
            testCoordinator.stop();
        }
    }

    private static void setField(Class<?> owner, Object target, String name, Object value) throws Exception {
        Field field = owner.getDeclaredField(name);
        field.setAccessible(true);
        field.set(target, value);
    }
}
