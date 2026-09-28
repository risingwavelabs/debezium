/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */

package io.debezium.connector.postgresql.connection;

import java.sql.SQLException;
import java.util.Optional;
import java.util.concurrent.atomic.AtomicBoolean;

import io.debezium.connector.postgresql.spi.SlotCreationResult;
import io.debezium.jdbc.JdbcConnection;

/**
 * Retains forced-shutdown state on the replication connection shared by the streaming source and
 * {@code PostgresConnectorTask}. This prevents the task's later cleanup from reopening a regular
 * JDBC connection to drop the slot after the source has force-aborted its sockets.
 */
public final class GuardedReplicationConnection implements ReplicationConnection {
    private final ReplicationConnection delegate;
    private final CloseDelegate closeDelegate;
    private final AtomicBoolean slotDropSuppressed = new AtomicBoolean();
    private volatile PostgresConnection.ConnectionTracker connectionTracker;

    public GuardedReplicationConnection(ReplicationConnection delegate) {
        this(delegate, closeDelegate(delegate));
    }

    GuardedReplicationConnection(ReplicationConnection delegate, CloseDelegate closeDelegate) {
        this.delegate = delegate;
        this.closeDelegate = closeDelegate;
    }

    public void suppressSlotDrop() {
        slotDropSuppressed.set(true);
    }

    public void setConnectionTracker(PostgresConnection.ConnectionTracker connectionTracker) {
        this.connectionTracker = connectionTracker;
    }

    public JdbcConnection jdbcConnection() {
        return delegate instanceof JdbcConnection ? (JdbcConnection) delegate : null;
    }

    public void close(boolean dropSlot) throws Exception {
        closeDelegate.close(dropSlot && !slotDropSuppressed.get());
    }

    @Override
    public void close() throws Exception {
        close(true);
    }

    @Override
    public ReplicationStream startStreaming(WalPositionLocator walPosition)
            throws SQLException, InterruptedException {
        return delegate.startStreaming(walPosition);
    }

    @Override
    public ReplicationStream startStreaming(Lsn offset, WalPositionLocator walPosition)
            throws SQLException, InterruptedException {
        return delegate.startStreaming(offset, walPosition);
    }

    @Override
    public Optional<SlotCreationResult> createReplicationSlot() throws SQLException {
        return delegate.createReplicationSlot();
    }

    @Override
    public void initConnection() throws SQLException, InterruptedException {
        delegate.initConnection();
    }

    @Override
    public boolean isConnected() throws SQLException {
        return delegate.isConnected();
    }

    @Override
    public void reconnect() throws SQLException {
        PostgresConnection.ConnectionTracker tracker = connectionTracker;
        if (tracker == null) {
            delegate.reconnect();
            return;
        }

        tracker.beforeConnect();
        try (PostgresConnection.ConnectionTrackingScope ignored = PostgresConnection.trackConnections(tracker)) {
            delegate.reconnect();
            JdbcConnection jdbcConnection = jdbcConnection();
            if (jdbcConnection != null) {
                tracker.capture(jdbcConnection.connection(false));
            }
        }
    }

    private static CloseDelegate closeDelegate(ReplicationConnection delegate) {
        if (delegate instanceof PostgresReplicationConnection) {
            return dropSlot -> ((PostgresReplicationConnection) delegate).close(dropSlot);
        }
        return ignored -> delegate.close();
    }

    @FunctionalInterface
    interface CloseDelegate {
        void close(boolean dropSlot) throws Exception;
    }
}
