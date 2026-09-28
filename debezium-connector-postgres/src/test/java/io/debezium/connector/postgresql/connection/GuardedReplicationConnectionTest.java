/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */

package io.debezium.connector.postgresql.connection;

import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertThrows;
import static org.junit.Assert.assertTrue;

import java.lang.reflect.Proxy;
import java.sql.Connection;
import java.sql.SQLException;
import java.util.concurrent.atomic.AtomicBoolean;

import org.junit.Test;

public class GuardedReplicationConnectionTest {

    @Test
    public void forcedShutdownSuppressesSlotDropDuringSourceAndTaskCleanup() throws Exception {
        AtomicBoolean dropSlot = new AtomicBoolean(true);
        GuardedReplicationConnection connection = new GuardedReplicationConnection(replicationConnection(), dropSlot::set);

        connection.suppressSlotDrop();
        connection.close(false); // Streaming source cleanup.
        assertFalse(dropSlot.get());

        dropSlot.set(true);
        connection.close(); // PostgresConnectorTask.doStop().
        assertFalse(dropSlot.get());
    }

    @Test
    public void gracefulTaskCleanupPreservesConfiguredSlotDrop() throws Exception {
        AtomicBoolean dropSlot = new AtomicBoolean(false);
        GuardedReplicationConnection connection = new GuardedReplicationConnection(replicationConnection(), dropSlot::set);

        connection.close();

        assertTrue(dropSlot.get());
    }

    @Test
    public void abortedReconnectDoesNotReachDelegate() {
        AtomicBoolean reconnected = new AtomicBoolean();
        ReplicationConnection delegate = (ReplicationConnection) Proxy.newProxyInstance(
                ReplicationConnection.class.getClassLoader(),
                new Class<?>[]{ ReplicationConnection.class },
                (proxy, method, args) -> {
                    if (method.getName().equals("reconnect")) {
                        reconnected.set(true);
                    }
                    return null;
                });
        GuardedReplicationConnection connection = new GuardedReplicationConnection(delegate);
        connection.setConnectionTracker(
                new PostgresConnection.ConnectionTracker() {
                    @Override
                    public void beforeConnect() throws SQLException {
                        throw new SQLException("Connection requested during forced shutdown");
                    }

                    @Override
                    public Connection capture(Connection connection) {
                        return connection;
                    }
                });

        assertThrows(SQLException.class, connection::reconnect);
        assertFalse(reconnected.get());
    }

    private static ReplicationConnection replicationConnection() {
        return (ReplicationConnection) Proxy.newProxyInstance(
                ReplicationConnection.class.getClassLoader(),
                new Class<?>[]{ ReplicationConnection.class },
                (proxy, method, args) -> null);
    }
}
