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
import java.util.Set;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicReference;

import org.apache.kafka.clients.producer.RecordMetadata;
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
    private static CountDownLatch callbackEntered;
    private static CountDownLatch releaseCallback;
    private static String blockedCallback;
    private static boolean failStop;
    private static final AtomicInteger lateRecordCallbacks = new AtomicInteger();
    private static final AtomicInteger lateBatchCallbacks = new AtomicInteger();
    private static final AtomicInteger taskStops = new AtomicInteger();
    private static final AtomicInteger taskCommits = new AtomicInteger();
    private static final AtomicInteger storeStops = new AtomicInteger();
    private static final AtomicBoolean failLateFlush = new AtomicBoolean();

    private enum Scenario {
        EARLY_ACK,
        FAIL_FLUSH,
        FINISH_BATCH,
        BLOCK_RECORD,
        BLOCK_BATCH,
        BLOCK_FLUSH,
        FAIL_STOP,
        CUSTOM_OFFSETS
    }

    @Test(timeout = 10000)
    public void lateAcknowledgmentIsPersistedAfterThePreStopCommit() throws Exception {
        lateAcknowledgment(Scenario.EARLY_ACK);
    }

    @Test(timeout = 10000)
    public void firstAcknowledgmentAfterTaskStopIsPersisted() throws Exception {
        lateAcknowledgment();
    }

    @Test(timeout = 10000)
    public void lateFlushFailureIsReportedAndStoreStillStops() throws Exception {
        lateAcknowledgment(Scenario.EARLY_ACK, Scenario.FAIL_FLUSH);
    }

    @Test(timeout = 10000)
    public void finishingBatchAfterStopDoesNotInvokeTaskCallbacks() throws Exception {
        lateAcknowledgment(Scenario.EARLY_ACK, Scenario.FINISH_BATCH);
    }

    @Test(timeout = 10000)
    public void failedStopStillClosesTheTaskCallbackBoundary() throws Exception {
        lateAcknowledgment(Scenario.EARLY_ACK, Scenario.FINISH_BATCH, Scenario.FAIL_STOP);
    }

    @Test(timeout = 10000)
    public void customOffsetsAfterStopArePersistedWithoutTaskCallbacks() throws Exception {
        lateAcknowledgment(Scenario.EARLY_ACK, Scenario.FINISH_BATCH, Scenario.CUSTOM_OFFSETS);
    }

    @Test(timeout = 10000)
    public void stopWaitsForAnInFlightRecordCallback() throws Exception {
        lateAcknowledgment(Scenario.EARLY_ACK, Scenario.FINISH_BATCH, Scenario.BLOCK_RECORD);
    }

    @Test(timeout = 10000)
    public void stopWaitsForAnInFlightBatchCommit() throws Exception {
        lateAcknowledgment(Scenario.EARLY_ACK, Scenario.FINISH_BATCH, Scenario.BLOCK_BATCH);
    }

    @Test(timeout = 10000)
    public void stopWaitsForFlushBeforeInvokingTheTaskCommitHook() throws Exception {
        lateAcknowledgment(Scenario.EARLY_ACK, Scenario.FINISH_BATCH, Scenario.BLOCK_FLUSH);
    }

    private void lateAcknowledgment(Scenario... scenarios) throws Exception {
        var scenario = Set.of(scenarios);
        boolean earlyAck = scenario.contains(Scenario.EARLY_ACK);
        boolean failFlush = scenario.contains(Scenario.FAIL_FLUSH);
        boolean finishBatch = scenario.contains(Scenario.FINISH_BATCH);
        boolean throwFromStop = scenario.contains(Scenario.FAIL_STOP);
        boolean customOffsets = scenario.contains(Scenario.CUSTOM_OFFSETS);
        String blockCallback = scenario.contains(Scenario.BLOCK_RECORD) ? "record"
                : scenario.contains(Scenario.BLOCK_BATCH) ? "batch"
                        : scenario.contains(Scenario.BLOCK_FLUSH) ? "flush" : "";
        callbackEntered = new CountDownLatch(1);
        releaseCallback = new CountDownLatch(1);
        blockedCallback = blockCallback;
        failStop = throwFromStop;
        lateRecordCallbacks.set(0);
        lateBatchCallbacks.set(0);
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
        props.setProperty(AsyncEngineConfig.TASK_MANAGEMENT_TIMEOUT_MS.name(), blockCallback.isEmpty() ? "5000" : "100");
        props.setProperty(AsyncEngineConfig.RECORD_PROCESSING_SHUTDOWN_TIMEOUT_MS.name(), "10");
        var success = new AtomicReference<Boolean>();
        var failure = new AtomicReference<Throwable>();
        var engine = (AsyncEmbeddedEngine<?>) new AsyncEmbeddedEngine.AsyncEngineBuilder<SourceRecord>()
                .using(props)
                .using((records, elapsed) -> true)
                .using((ok, message, error) -> {
                    success.set(ok);
                    failure.set(error);
                })
                .notifying((records, committer) -> {
                    if (earlyAck) {
                        committer.markProcessed(records.get(0));
                    }
                    if (blockCallback.equals("batch") || blockCallback.equals("flush")) {
                        committer.markBatchFinished();
                    }
                    consuming.countDown();
                    // Task stop must be allowed to release the consumer; moving stop after
                    // consumer termination would deadlock this dependency.
                    awaitUninterruptibly(stopped);
                    if (customOffsets) {
                        var offsets = committer.buildOffsets();
                        offsets.set("offset", 3);
                        committer.markProcessed(records.get(1), offsets);
                    }
                    else {
                        committer.markProcessed(records.get(1));
                    }
                    if (finishBatch) {
                        committer.markBatchFinished();
                    }
                }).build();
        var threads = Executors.newSingleThreadExecutor();
        try {
            var run = threads.submit(engine);
            if (blockCallback.isEmpty()) {
                assertThat(consuming.await(5, TimeUnit.SECONDS)).isTrue();
            }
            else {
                assertThat(callbackEntered.await(5, TimeUnit.SECONDS)).isTrue();
            }
            engine.close();
            if (!blockCallback.isEmpty()) {
                assertThat(taskStops.get()).as("stop must wait for the active task callback").isZero();
                assertThat(engine.hasPendingCleanup()).isTrue();
                releaseCallback.countDown();
            }
            run.get(5, TimeUnit.SECONDS);
            engine.cleanupCompletion().toCompletableFuture().get(5, TimeUnit.SECONDS);
            assertThat(engine.hasPendingCleanup()).isFalse();
            assertThat(lateBatchCallbacks.get()).as("no task.commit after stop").isZero();
            assertThat(lateRecordCallbacks.get()).as("no task.commitRecord after stop").isZero();
            assertThat(taskStops.get()).isEqualTo(1);
            if (blockCallback.equals("record")) {
                // The cleanup deadline can interrupt the pre-stop flush after the hook exits.
                // That is a reported failure, not permission to commit after task shutdown.
                assertThat(taskCommits.get()).isBetween(0, 1);
                if (taskCommits.get() == 0) {
                    assertThat(engine.getShutdownFailure()).isInstanceOf(InterruptedException.class);
                }
            }
            else {
                assertThat(taskCommits.get()).isEqualTo(earlyAck ? 1 : 0);
            }
            assertThat(storeStops.get()).isEqualTo(1);
            if (throwFromStop) {
                assertThat(success.get()).isFalse();
                assertThat(failure.get()).hasMessage("task stop failed");
                assertThat(readPersistedOffset(props)).containsEntry("offset", 2L);
            }
            else if (failFlush) {
                assertThat(success.get()).isFalse();
                assertThat(failure.get()).hasMessage("late offset flush failed");
                assertThat(engine.getShutdownFailure()).isSameAs(failure.get());
            }
            else {
                if (blockCallback.isEmpty()) {
                    assertThat(success.get()).isTrue();
                    assertThat(failure.get()).isNull();
                    assertThat(engine.getShutdownFailure()).isNull();
                }
                else if (engine.getShutdownFailure() != null) {
                    assertThat(engine.getShutdownFailure()).isInstanceOf(InterruptedException.class);
                }
                assertThat(readPersistedOffset(props)).containsEntry("offset", customOffsets ? 3L : 2L);
            }
        }
        finally {
            releaseCallback.countDown();
            stopped.countDown();
            if (!engine.isShutdownRequested()) {
                engine.close();
            }
            threads.shutdownNow();
            engine.cleanupCompletion().toCompletableFuture().get(5, TimeUnit.SECONDS);
        }
    }

    private static void awaitUninterruptibly(CountDownLatch latch) {
        boolean released = false;
        while (!released) {
            try {
                latch.await();
                released = true;
            }
            catch (InterruptedException ignored) {
                // Model a consumer or task hook that outlives the shutdown wait.
            }
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
        public void commitRecord(SourceRecord record, RecordMetadata metadata) {
            if (blockedCallback.equals("record")) {
                callbackEntered.countDown();
                awaitUninterruptibly(releaseCallback);
            }
            if (stopped.getCount() == 0) {
                lateRecordCallbacks.incrementAndGet();
            }
        }

        @Override
        public void commit() {
            if (blockedCallback.equals("batch")) {
                callbackEntered.countDown();
                awaitUninterruptibly(releaseCallback);
            }
            if (stopped.getCount() == 0) {
                lateBatchCallbacks.incrementAndGet();
                throw new IllegalStateException("task already stopped");
            }
            taskCommits.incrementAndGet();
        }

        @Override
        public void stop() {
            taskStops.incrementAndGet();
            stopped.countDown();
            if (failStop) {
                throw new IllegalStateException("task stop failed");
            }
        }
    }

    public static class Store extends FileOffsetBackingStore {
        public Store() {
            super(KafkaConnectUtil.converterForOffsetStore());
        }

        @Override
        public Future<Void> set(Map<ByteBuffer, ByteBuffer> values, Callback<Void> callback) {
            if (blockedCallback.equals("flush") && stopped.getCount() != 0) {
                callbackEntered.countDown();
                awaitUninterruptibly(releaseCallback);
            }
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
