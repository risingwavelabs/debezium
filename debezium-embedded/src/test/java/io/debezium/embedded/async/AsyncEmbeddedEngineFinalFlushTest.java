/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.debezium.embedded.async;

import static org.assertj.core.api.Assertions.assertThat;

import java.nio.ByteBuffer;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicReference;

import org.apache.kafka.connect.connector.Task;
import org.apache.kafka.connect.data.Schema;
import org.apache.kafka.connect.json.JsonConverter;
import org.apache.kafka.connect.runtime.standalone.StandaloneConfig;
import org.apache.kafka.connect.source.SourceRecord;
import org.apache.kafka.connect.storage.FileOffsetBackingStore;
import org.apache.kafka.connect.storage.OffsetStorageReaderImpl;
import org.apache.kafka.connect.util.Callback;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TemporaryFolder;

import io.debezium.connector.simple.SimpleSourceConnector;
import io.debezium.embedded.KafkaConnectUtil;

public class AsyncEmbeddedEngineFinalFlushTest {
    @Rule
    public TemporaryFolder directory = new TemporaryFolder();

    private static final Map<String, Integer> PARTITION = Map.of("partition", 1);
    private static CountDownLatch consuming;
    private static CountDownLatch stopped;
    private static final AtomicInteger taskStops = new AtomicInteger();
    private static final AtomicInteger taskCommits = new AtomicInteger();
    private static final AtomicInteger storeStops = new AtomicInteger();
    private static final AtomicBoolean failLateFlush = new AtomicBoolean();

    @Test(timeout = 10000)
    public void lateAcknowledgmentIsPersistedAfterThePreStopCommit() throws Exception {
        lateAcknowledgment(true, false);
    }

    @Test(timeout = 10000)
    public void firstAcknowledgmentAfterTaskStopIsPersisted() throws Exception {
        lateAcknowledgment(false, false);
    }

    @Test(timeout = 10000)
    public void lateFlushFailureIsReportedAndStoreStillStops() throws Exception {
        lateAcknowledgment(true, true);
    }

    private void lateAcknowledgment(boolean earlyAck, boolean failFlush) throws Exception {
        consuming = new CountDownLatch(1);
        stopped = new CountDownLatch(1);
        taskStops.set(0);
        taskCommits.set(0);
        storeStops.set(0);
        failLateFlush.set(failFlush);
        var props = new Properties();
        props.setProperty("name", "late-ack");
        props.setProperty("connector.class", Connector.class.getName());
        props.setProperty("offset.storage", Store.class.getName());
        props.setProperty("offset.storage.file.filename", directory.newFolder().toPath().resolve("offsets").toString());
        props.setProperty(AsyncEngineConfig.TASK_MANAGEMENT_TIMEOUT_MS.name(), "5000");
        props.setProperty(AsyncEngineConfig.RECORD_PROCESSING_SHUTDOWN_TIMEOUT_MS.name(), "10");
        var success = new AtomicReference<Boolean>();
        var failure = new AtomicReference<Throwable>();
        var engine = (AsyncEmbeddedEngine<?>) new AsyncEmbeddedEngine.AsyncEngineBuilder<SourceRecord>()
                .using(props)
                .using((ok, message, error) -> {
                    success.set(ok);
                    failure.set(error);
                })
                .notifying((records, committer) -> {
                    if (earlyAck) {
                        committer.markProcessed(records.get(0));
                    }
                    consuming.countDown();
                    // Task stop must be allowed to release the consumer; moving stop after
                    // consumer termination would deadlock this dependency.
                    boolean released = false;
                    while (!released) {
                        try {
                            stopped.await();
                            released = true;
                        }
                        catch (InterruptedException ignored) {
                        }
                    }
                    committer.markProcessed(records.get(1));
                    // No markBatchFinished(): shutdown must persist this acknowledgment.
                }).build();
        var threads = Executors.newSingleThreadExecutor();
        try {
            var run = threads.submit(engine);
            assertThat(consuming.await(5, TimeUnit.SECONDS)).isTrue();
            engine.close();
            run.get(5, TimeUnit.SECONDS);
            engine.cleanupCompletion().toCompletableFuture().get(5, TimeUnit.SECONDS);
            assertThat(engine.hasPendingCleanup()).isFalse();
            assertThat(taskStops.get()).isEqualTo(1);
            assertThat(taskCommits.get()).isEqualTo(earlyAck ? 1 : 0);
            assertThat(storeStops.get()).isEqualTo(1);
            if (failFlush) {
                assertThat(success.get()).isFalse();
                assertThat(failure.get()).hasMessage("late offset flush failed");
                assertThat(engine.getShutdownFailure()).isSameAs(failure.get());
            }
            else {
                assertThat(success.get()).isTrue();
                assertThat(failure.get()).isNull();
                assertThat(readPersistedOffset(props)).containsEntry("offset", 2L);
            }
        }
        finally {
            stopped.countDown();
            threads.shutdownNow();
        }
    }

    private Map<String, Object> readPersistedOffset(Properties props) {
        var store = KafkaConnectUtil.fileOffsetBackingStore();
        store.configure(new StandaloneConfig(Map.of(
                "key.converter", JsonConverter.class.getName(),
                "value.converter", JsonConverter.class.getName(),
                "offset.storage.file.filename", props.getProperty("offset.storage.file.filename"))));
        var key = new JsonConverter();
        var value = new JsonConverter();
        key.configure(Map.of("schemas.enable", false), true);
        value.configure(Map.of("schemas.enable", false), false);
        store.start();
        try (var reader = new OffsetStorageReaderImpl(store, "late-ack", key, value)) {
            return reader.offset(PARTITION);
        }
        finally {
            store.stop();
        }
    }

    public static class Connector extends SimpleSourceConnector {
        @Override
        public Class<? extends Task> taskClass() {
            return LateAckTask.class;
        }
    }

    public static class LateAckTask extends DebeziumAsyncEngineTestUtils.NoOpTask {
        private boolean emitted;

        @Override
        public List<SourceRecord> poll() throws InterruptedException {
            if (!emitted) {
                emitted = true;
                return List.of(record(1), record(2));
            }
            return super.poll();
        }

        private SourceRecord record(int offset) {
            return new SourceRecord(PARTITION, Map.of("offset", offset), "test", Schema.STRING_SCHEMA, "value");
        }

        @Override
        public void commit() {
            assertThat(stopped.getCount()).as("task commit must not run after task stop").isEqualTo(1);
            taskCommits.incrementAndGet();
        }

        @Override
        public void stop() {
            taskStops.incrementAndGet();
            stopped.countDown();
        }
    }

    public static class Store extends FileOffsetBackingStore {
        public Store() {
            super(KafkaConnectUtil.converterForOffsetStore());
        }

        @Override
        public Future<Void> set(Map<ByteBuffer, ByteBuffer> values, Callback<Void> callback) {
            if (failLateFlush.get() && stopped.getCount() == 0) {
                var failure = new IllegalStateException("late offset flush failed");
                callback.onCompletion(failure, null);
                return CompletableFuture.failedFuture(failure);
            }
            return super.set(values, callback);
        }

        @Override
        public void stop() {
            storeStops.incrementAndGet();
            super.stop();
        }
    }
}
