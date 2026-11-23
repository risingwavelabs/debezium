/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.debezium.embedded.async;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

import java.nio.ByteBuffer;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicReference;

import org.apache.kafka.connect.connector.Task;
import org.apache.kafka.connect.data.Schema;
import org.apache.kafka.connect.json.JsonConverter;
import org.apache.kafka.connect.runtime.ConnectorConfig;
import org.apache.kafka.connect.runtime.standalone.StandaloneConfig;
import org.apache.kafka.connect.source.SourceRecord;
import org.apache.kafka.connect.storage.FileOffsetBackingStore;
import org.apache.kafka.connect.util.Callback;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TemporaryFolder;

import io.debezium.connector.simple.SimpleSourceConnector;
import io.debezium.embedded.KafkaConnectUtil;
import io.debezium.engine.DebeziumEngine;
import io.debezium.engine.format.Json;
import io.debezium.engine.format.KeyValueHeaderChangeEventFormat;

public class AsyncEmbeddedEngineShutdownTest {
    @Rule
    public TemporaryFolder temporaryFolder = new TemporaryFolder();

    public static class BothFailConnector extends SimpleSourceConnector {
        @Override
        public Class<? extends Task> taskClass() {
            return BothFailTask.class;
        }
    }

    public static class BothFailTask extends DebeziumAsyncEngineTestUtils.NoOpTask {
        @Override
        public List<SourceRecord> poll() {
            throw new IllegalArgumentException("poll failed");
        }

        @Override
        public void stop() {
            throw new IllegalStateException("stop failed");
        }
    }

    @Test
    public void callbackRetainsCleanupFailureWhenPollingAlsoFails() throws Exception {
        AtomicReference<Throwable> reported = new AtomicReference<>();
        try (DebeziumEngine<SourceRecord> engine = build(BothFailConnector.class, (success, message, error) -> reported.set(error))) {
            engine.run();
            assertThat(reported.get()).isInstanceOf(IllegalArgumentException.class);
            assertThat(reported.get().getSuppressed()).as("cleanup failure must remain visible to the completion callback")
                    .extracting(Throwable::getMessage).contains("stop failed");
        }
        catch (IllegalStateException alreadyStopped) {
            // run() stops the engine itself.
            if (!alreadyStopped.getMessage().contains("already shut down")) {
                throw alreadyStopped;
            }
        }
    }

    public static class SlowFailConnector extends SimpleSourceConnector {
        static CountDownLatch stopping;
        static CountDownLatch release;
        static CountDownLatch polling;

        @Override
        public Class<? extends Task> taskClass() {
            return SlowFailTask.class;
        }

        @Override
        public void stop() {
            stopping.countDown();
            try {
                release.await();
            }
            catch (InterruptedException e) {
                Thread.currentThread().interrupt();
            }
            throw new IllegalStateException("connector cleanup failed");
        }
    }

    public static class SlowFailTask extends DebeziumAsyncEngineTestUtils.NoOpTask {
        @Override
        public List<SourceRecord> poll() throws InterruptedException {
            SlowFailConnector.polling.countDown();
            return super.poll();
        }
    }

    @Test
    public void callbackMustNotReportSuccessWhileCleanupIsUnfinished() throws Exception {
        unfinishedCleanup(false);
    }

    @Test
    public void interruptedShutdownWaitMustNotReportSuccess() throws Exception {
        unfinishedCleanup(true);
    }

    private void unfinishedCleanup(boolean interrupt) throws Exception {
        SlowFailConnector.stopping = new CountDownLatch(1);
        SlowFailConnector.release = new CountDownLatch(1);
        SlowFailConnector.polling = new CountDownLatch(1);
        CountDownLatch completed = new CountDownLatch(1);
        AtomicReference<Boolean> reportedSuccess = new AtomicReference<>();
        AtomicReference<Throwable> reportedError = new AtomicReference<>();
        AtomicReference<Thread> engineThread = new AtomicReference<>();
        DebeziumEngine<SourceRecord> engine = build(SlowFailConnector.class, (success, message, error) -> {
            reportedSuccess.set(success);
            reportedError.set(error);
            completed.countDown();
        });
        var threads = Executors.newFixedThreadPool(2);
        try {
            var run = threads.submit(() -> {
                engineThread.set(Thread.currentThread());
                engine.run();
            });
            assertThat(SlowFailConnector.polling.await(5, TimeUnit.SECONDS)).isTrue();
            var close = threads.submit(() -> {
                engine.close();
                return null;
            });
            assertThat(SlowFailConnector.stopping.await(5, TimeUnit.SECONDS)).isTrue();
            if (interrupt) {
                engineThread.get().interrupt();
            }
            assertThat(completed.await(5, TimeUnit.SECONDS)).isTrue();
            assertThat(reportedError.get()).isInstanceOf(interrupt ? InterruptedException.class : java.util.concurrent.TimeoutException.class);
            SlowFailConnector.release.countDown();
            close.get(5, TimeUnit.SECONDS);
            run.get(5, TimeUnit.SECONDS);
            assertThat(reportedSuccess.get()).as("unfinished/failed shutdown must not be reported as success").isFalse();
            org.awaitility.Awaitility.await().atMost(5, TimeUnit.SECONDS)
                    .until(() -> !((AsyncEmbeddedEngine<?>) engine).hasPendingCleanup());
            assertThat(((AsyncEmbeddedEngine<?>) engine).getShutdownFailure()).hasMessage("connector cleanup failed");
        }
        finally {
            SlowFailConnector.release.countDown();
            threads.shutdownNow();
        }
    }

    public static class RetryConnector extends SimpleSourceConnector {
        @Override
        public Class<? extends Task> taskClass() {
            return RetryTask.class;
        }
    }

    public static class RetryTask extends DebeziumAsyncEngineTestUtils.NoOpTask {
        static CountDownLatch polling;
        static CountDownLatch release;
        static AtomicInteger stops = new AtomicInteger();

        @Override
        public List<SourceRecord> poll() throws InterruptedException {
            polling.countDown();
            return super.poll();
        }

        @Override
        public void stop() {
            if (stops.incrementAndGet() == 1) {
                throw new IllegalStateException("first stop failed");
            }
            boolean released = false;
            while (!released) {
                try {
                    release.await();
                    released = true;
                }
                catch (InterruptedException ignored) {
                    // Model native cleanup that does not respond to interruption.
                }
            }
        }
    }

    @Test
    public void timedOutTaskRetryIsNotRunConcurrently() throws Exception {
        RetryTask.stops.set(0);
        RetryTask.polling = new CountDownLatch(1);
        RetryTask.release = new CountDownLatch(1);
        CountDownLatch completed = new CountDownLatch(1);
        AsyncEmbeddedEngine<?> engine = (AsyncEmbeddedEngine<?>) build(RetryConnector.class,
                (success, message, error) -> completed.countDown());
        var threads = Executors.newSingleThreadExecutor();
        try {
            var run = threads.submit(engine);
            assertThat(RetryTask.polling.await(5, TimeUnit.SECONDS)).isTrue();
            engine.close();
            assertThat(completed.await(5, TimeUnit.SECONDS)).isTrue();
            run.get(5, TimeUnit.SECONDS);
            assertThat(engine.getShutdownFailure()).hasMessage("first stop failed");
            assertThatThrownBy(engine::retryFailedTaskStops).isInstanceOf(TimeoutException.class);
            assertThatThrownBy(engine::retryFailedTaskStops).isInstanceOf(TimeoutException.class);
            assertThat(RetryTask.stops.get()).isEqualTo(2);
            RetryTask.release.countDown();
            engine.retryFailedTaskStops();
            assertThat(engine.getShutdownFailure()).isNull();
            engine.retryFailedTaskStops();
            assertThat(RetryTask.stops.get()).isEqualTo(2);
        }
        finally {
            RetryTask.release.countDown();
            threads.shutdownNow();
        }
    }

    public static class FailingCloseConverter extends JsonConverter {
        static boolean failWithError;

        @Override
        public void close() {
            if (failWithError) {
                throw new AssertionError("header converter close failed");
            }
            throw new IllegalStateException("header converter close failed");
        }
    }

    @Test(timeout = 10000)
    public void converterCloseExceptionDoesNotBlockTaskCleanupRetry() throws Exception {
        converterCloseFailure(false);
    }

    @Test(timeout = 10000)
    public void converterCloseErrorDoesNotBlockTaskCleanupRetry() throws Exception {
        converterCloseFailure(true);
    }

    private void converterCloseFailure(boolean failWithError) throws Exception {
        FailingCloseConverter.failWithError = failWithError;
        RetryTask.stops.set(0);
        RetryTask.polling = new CountDownLatch(1);
        RetryTask.release = new CountDownLatch(0);
        var success = new AtomicReference<Boolean>();
        var error = new AtomicReference<Throwable>();
        Properties props = new Properties();
        props.setProperty(ConnectorConfig.NAME_CONFIG, "converter-close-failure");
        props.setProperty(ConnectorConfig.CONNECTOR_CLASS_CONFIG, RetryConnector.class.getName());
        props.setProperty(StandaloneConfig.OFFSET_STORAGE_FILE_FILENAME_CONFIG,
                temporaryFolder.newFolder().toPath().resolve("offsets").toString());
        props.setProperty(AsyncEngineConfig.TASK_MANAGEMENT_TIMEOUT_MS.name(), "200");
        props.setProperty("header.converter.class", FailingCloseConverter.class.getName());
        props.setProperty("transforms", "close");
        props.setProperty("transforms.close.type", AsyncEmbeddedEngineTest.CloseTestTransform.class.getName());
        var engine = (AsyncEmbeddedEngine<?>) new AsyncEmbeddedEngine.AsyncEngineBuilder<>(
                KeyValueHeaderChangeEventFormat.of(Json.class, Json.class, Json.class))
                .using(props)
                .using((ok, message, failure) -> {
                    success.set(ok);
                    error.set(failure);
                })
                .notifying((records, committer) -> {
                }).build();
        var threads = Executors.newSingleThreadExecutor();
        try {
            var run = threads.submit(engine);
            assertThat(RetryTask.polling.await(5, TimeUnit.SECONDS)).isTrue();
            engine.close();
            run.get(5, TimeUnit.SECONDS);
            assertThat(success.get()).isFalse();
            assertThat(error.get()).isInstanceOf(failWithError ? AssertionError.class : IllegalStateException.class)
                    .hasMessage("header converter close failed");
            assertThat(error.get().getSuppressed()).extracting(Throwable::getMessage).contains("first stop failed");
            assertThat(AsyncEmbeddedEngineTest.CloseTestTransform.wasClosed).isTrue();
            assertThat(RetryTask.stops.get()).isEqualTo(1);
            assertThat(engine.hasPendingCleanup()).isTrue();

            // The task retry must run, while the non-task converter failure remains visible.
            assertThatThrownBy(engine::retryFailedTaskStops).isInstanceOf(java.io.IOException.class).hasCause(error.get());
            assertThat(RetryTask.stops.get()).isEqualTo(2);
            assertThat(engine.getShutdownFailure()).isSameAs(error.get());
            assertThat(engine.hasPendingCleanup()).isFalse();
            assertThatThrownBy(engine::retryFailedTaskStops).isInstanceOf(java.io.IOException.class).hasCause(error.get());
            assertThat(RetryTask.stops.get()).isEqualTo(2);
        }
        finally {
            threads.shutdownNow();
        }
    }

    public static class BlockedPollConnector extends SimpleSourceConnector {
        @Override
        public Class<? extends Task> taskClass() {
            return BlockedPollTask.class;
        }
    }

    public static class BlockedPollTask extends DebeziumAsyncEngineTestUtils.NoOpTask {
        static CountDownLatch polling;
        static CountDownLatch release;
        static AtomicInteger stops = new AtomicInteger();

        @Override
        public List<SourceRecord> poll() {
            polling.countDown();
            awaitIgnoringInterrupts(release);
            return List.of();
        }

        @Override
        public void stop() {
            if (stops.incrementAndGet() == 1) {
                throw new IllegalStateException("first stop failed");
            }
            release.countDown();
        }
    }

    @Test(timeout = 10000)
    public void cleanupRetryUnblocksPollingThatIgnoresInterrupts() throws Exception {
        BlockedPollTask.polling = new CountDownLatch(1);
        BlockedPollTask.release = new CountDownLatch(1);
        BlockedPollTask.stops.set(0);
        var engine = (AsyncEmbeddedEngine<?>) build(BlockedPollConnector.class, (success, message, error) -> {
        });
        var threads = Executors.newSingleThreadExecutor();
        try {
            var run = threads.submit(engine);
            assertThat(BlockedPollTask.polling.await(5, TimeUnit.SECONDS)).isTrue();
            engine.close();
            assertThat(BlockedPollTask.stops.get()).isEqualTo(1);
            assertThat(BlockedPollTask.release.getCount()).isEqualTo(1);
            engine.retryFailedTaskStops();
            run.get(5, TimeUnit.SECONDS);
            assertThat(BlockedPollTask.stops.get()).isEqualTo(2);
            assertThat(engine.getShutdownFailure()).isNull();
        }
        finally {
            BlockedPollTask.release.countDown();
            threads.shutdownNow();
        }
    }

    public static class SlowStopConnector extends SimpleSourceConnector {
        @Override
        public Class<? extends Task> taskClass() {
            return SlowStopTask.class;
        }
    }

    public static class SlowStopTask extends DebeziumAsyncEngineTestUtils.NoOpTask {
        static CountDownLatch polling;
        static CountDownLatch release;
        static AtomicInteger stops = new AtomicInteger();

        @Override
        public List<SourceRecord> poll() throws InterruptedException {
            polling.countDown();
            return super.poll();
        }

        @Override
        public void stop() {
            stops.incrementAndGet();
            awaitIgnoringInterrupts(release);
        }
    }

    @Test(timeout = 10000)
    public void retryWaitsForInitialCleanupWithoutStoppingConcurrently() throws Exception {
        SlowStopTask.polling = new CountDownLatch(1);
        SlowStopTask.release = new CountDownLatch(1);
        SlowStopTask.stops.set(0);
        var engine = (AsyncEmbeddedEngine<?>) build(SlowStopConnector.class, (success, message, error) -> {
        });
        var threads = Executors.newSingleThreadExecutor();
        try {
            var run = threads.submit(engine);
            assertThat(SlowStopTask.polling.await(5, TimeUnit.SECONDS)).isTrue();
            engine.close();
            run.get(5, TimeUnit.SECONDS);
            assertThatThrownBy(engine::retryFailedTaskStops).isInstanceOf(TimeoutException.class);
            assertThat(SlowStopTask.stops.get()).isEqualTo(1);
            SlowStopTask.release.countDown();
            engine.retryFailedTaskStops();
            assertThat(SlowStopTask.stops.get()).isEqualTo(1);
            assertThat(engine.getShutdownFailure()).isNull();
        }
        finally {
            SlowStopTask.release.countDown();
            threads.shutdownNow();
        }
    }

    private static void awaitIgnoringInterrupts(CountDownLatch latch) {
        boolean released = false;
        while (!released) {
            try {
                latch.await();
                released = true;
            }
            catch (InterruptedException ignored) {
                // Model a native operation which only resource cleanup can unblock.
            }
        }
    }

    public static class CommitErrorConnector extends SimpleSourceConnector {
        @Override
        public Class<? extends Task> taskClass() {
            return CommitErrorTask.class;
        }
    }

    public static class CommitErrorTask extends DebeziumAsyncEngineTestUtils.NoOpTask {
        static AtomicInteger stops = new AtomicInteger();
        private boolean emitted;

        @Override
        public List<SourceRecord> poll() throws InterruptedException {
            if (!emitted) {
                emitted = true;
                return List.of(new SourceRecord(Map.of("partition", 1), Map.of("offset", 1), "test", Schema.STRING_SCHEMA, "value"));
            }
            Thread.sleep(10);
            return List.of();
        }

        @Override
        public void commit() {
            throw new AssertionError("final commit failed");
        }

        @Override
        public void stop() {
            stops.incrementAndGet();
        }
    }

    @Test(timeout = 10000)
    public void finalCommitErrorRemainsVisibleAfterSuccessfulTaskCleanup() throws Exception {
        finalCommitFailure(CommitErrorConnector.class, Map.of(), AssertionError.class, "final commit failed");
    }

    public static class CommitExceptionConnector extends SimpleSourceConnector {
        @Override
        public Class<? extends Task> taskClass() {
            return CommitExceptionTask.class;
        }
    }

    public static class CommitExceptionTask extends CommitErrorTask {
        @Override
        public void commit() {
            throw new IllegalStateException("final commit exception");
        }
    }

    public static class FailedFlushStore extends FileOffsetBackingStore {
        static boolean nullFuture;

        public FailedFlushStore() {
            super(KafkaConnectUtil.converterForOffsetStore());
        }

        @Override
        public Future<Void> set(Map<ByteBuffer, ByteBuffer> values, Callback<Void> callback) {
            var failure = new IllegalStateException("offset flush failed");
            callback.onCompletion(failure, null);
            return nullFuture ? null : CompletableFuture.failedFuture(failure);
        }
    }

    @Test(timeout = 10000)
    public void finalCommitExceptionRemainsVisibleAfterSuccessfulTaskCleanup() throws Exception {
        finalCommitFailure(CommitExceptionConnector.class, Map.of(), IllegalStateException.class, "final commit exception");
    }

    @Test(timeout = 10000)
    public void failedFinalFlushIsReported() throws Exception {
        FailedFlushStore.nullFuture = false;
        finalCommitFailure(CommitErrorConnector.class, Map.of("offset.storage", FailedFlushStore.class.getName()),
                IllegalStateException.class, "offset flush failed");
    }

    @Test(timeout = 10000)
    public void nullFinalFlushIsReported() throws Exception {
        FailedFlushStore.nullFuture = true;
        finalCommitFailure(CommitErrorConnector.class, Map.of("offset.storage", FailedFlushStore.class.getName()),
                IllegalStateException.class, "offset flush failed");
    }

    private void finalCommitFailure(Class<?> connector, Map<String, String> overrides, Class<? extends Throwable> failureType, String message)
            throws Exception {
        CommitErrorTask.stops.set(0);
        var consumed = new CountDownLatch(1);
        var success = new AtomicReference<Boolean>();
        var error = new AtomicReference<Throwable>();
        var engine = (AsyncEmbeddedEngine<?>) build(connector, (ok, completionMessage, failure) -> {
            success.set(ok);
            error.set(failure);
        }, (records, committer) -> {
            for (SourceRecord record : records) {
                committer.markProcessed(record);
            }
            // Leave the final commit to shutdown, rather than committing the batch here.
            consumed.countDown();
        }, overrides);
        var threads = Executors.newSingleThreadExecutor();
        try {
            var run = threads.submit(engine);
            assertThat(consumed.await(5, TimeUnit.SECONDS)).isTrue();
            engine.close();
            run.get(5, TimeUnit.SECONDS);
            assertThat(success.get()).isFalse();
            assertThat(error.get()).isInstanceOf(failureType).hasMessage(message);
            assertThat(engine.getShutdownFailure()).isSameAs(error.get());
            assertThat(CommitErrorTask.stops.get()).isEqualTo(1);
            assertThat(engine.hasPendingCleanup()).isFalse();
            assertThatThrownBy(engine::retryFailedTaskStops).isInstanceOf(java.io.IOException.class)
                    .hasCause(error.get());
            assertThat(engine.getShutdownFailure()).isSameAs(error.get());
            assertThat(CommitErrorTask.stops.get()).isEqualTo(1);
        }
        finally {
            threads.shutdownNow();
        }
    }

    public static class NoOffsetsConnector extends SimpleSourceConnector {
        @Override
        public Class<? extends Task> taskClass() {
            return NoOffsetsTask.class;
        }
    }

    public static class NoOffsetsTask extends DebeziumAsyncEngineTestUtils.NoOpTask {
        static CountDownLatch polling;

        @Override
        public List<SourceRecord> poll() throws InterruptedException {
            polling.countDown();
            return super.poll();
        }

        @Override
        public void commit() {
            throw new AssertionError("No offsets should mean no final commit");
        }
    }

    @Test(timeout = 10000)
    public void nothingToFlushIsSuccessfulShutdown() throws Exception {
        NoOffsetsTask.polling = new CountDownLatch(1);
        var success = new AtomicReference<Boolean>();
        var error = new AtomicReference<Throwable>();
        var engine = (AsyncEmbeddedEngine<?>) build(NoOffsetsConnector.class, (ok, message, failure) -> {
            success.set(ok);
            error.set(failure);
        });
        var threads = Executors.newSingleThreadExecutor();
        try {
            var run = threads.submit(engine);
            assertThat(NoOffsetsTask.polling.await(5, TimeUnit.SECONDS)).isTrue();
            engine.close();
            run.get(5, TimeUnit.SECONDS);
            assertThat(success.get()).isTrue();
            assertThat(error.get()).isNull();
            assertThat(engine.getShutdownFailure()).isNull();
        }
        finally {
            threads.shutdownNow();
        }
    }

    public static class LateStartConnector extends SimpleSourceConnector {
        @Override
        public Class<? extends Task> taskClass() {
            return LateStartTask.class;
        }
    }

    public static class LateStartTask extends DebeziumAsyncEngineTestUtils.NoOpTask {
        static CountDownLatch entered;
        static CountDownLatch release;
        static CountDownLatch cleaned;
        static AtomicBoolean resourceOpen = new AtomicBoolean();
        static AtomicInteger stops = new AtomicInteger();
        static boolean failStart;

        @Override
        public void start(Map<String, String> props) {
            entered.countDown();
            awaitIgnoringInterrupts(release);
            resourceOpen.set(true);
            if (failStart) {
                throw new IllegalStateException("late startup failure");
            }
        }

        @Override
        public void stop() {
            stops.incrementAndGet();
            resourceOpen.set(false);
            cleaned.countDown();
        }
    }

    @Test(timeout = 10000)
    public void cleanupWaitsForTimedOutStartupToFinish() throws Exception {
        lateStartup(false);
    }

    @Test(timeout = 10000)
    public void cleanupWaitsForTimedOutStartupToFail() throws Exception {
        lateStartup(true);
    }

    private void lateStartup(boolean failStart) throws Exception {
        LateStartTask.entered = new CountDownLatch(1);
        LateStartTask.release = new CountDownLatch(1);
        LateStartTask.cleaned = new CountDownLatch(1);
        LateStartTask.resourceOpen.set(false);
        LateStartTask.stops.set(0);
        LateStartTask.failStart = failStart;
        var success = new AtomicReference<Boolean>();
        var engine = (AsyncEmbeddedEngine<?>) build(LateStartConnector.class, (ok, message, error) -> success.set(ok));
        var threads = Executors.newSingleThreadExecutor();
        try {
            var run = threads.submit(engine);
            assertThat(LateStartTask.entered.await(5, TimeUnit.SECONDS)).isTrue();
            run.get(5, TimeUnit.SECONDS);
            assertThat(success.get()).isFalse();
            assertThat(engine.getShutdownFailure()).isNotNull();
            assertThat(LateStartTask.stops.get()).isZero();
            assertThat(engine.hasPendingCleanup()).isTrue();
            assertThatThrownBy(engine::retryFailedTaskStops).isInstanceOf(TimeoutException.class);
            assertThat(LateStartTask.stops.get()).isZero();
            LateStartTask.release.countDown();
            // Cleanup must happen automatically after start() returns, without another retry.
            assertThat(LateStartTask.cleaned.await(5, TimeUnit.SECONDS)).isTrue();
            assertThat(LateStartTask.resourceOpen.get()).isFalse();
            engine.retryFailedTaskStops();
            assertThat(engine.getShutdownFailure()).isNull();
            assertThat(LateStartTask.stops.get()).isEqualTo(1);
            assertThat(engine.hasPendingCleanup()).isFalse();
        }
        finally {
            LateStartTask.release.countDown();
            threads.shutdownNow();
        }
    }

    private DebeziumEngine<SourceRecord> build(Class<?> connector, DebeziumEngine.CompletionCallback callback) throws Exception {
        return build(connector, callback, (records, committer) -> {
        });
    }

    private DebeziumEngine<SourceRecord> build(Class<?> connector, DebeziumEngine.CompletionCallback callback,
                                               DebeziumEngine.ChangeConsumer<SourceRecord> consumer)
            throws Exception {
        return build(connector, callback, consumer, Map.of());
    }

    private DebeziumEngine<SourceRecord> build(Class<?> connector, DebeziumEngine.CompletionCallback callback,
                                               DebeziumEngine.ChangeConsumer<SourceRecord> consumer, Map<String, String> overrides)
            throws Exception {
        Properties props = new Properties();
        props.setProperty(ConnectorConfig.NAME_CONFIG, "review-37");
        props.setProperty(ConnectorConfig.TASKS_MAX_CONFIG, "1");
        props.setProperty(ConnectorConfig.CONNECTOR_CLASS_CONFIG, connector.getName());
        props.setProperty(StandaloneConfig.OFFSET_STORAGE_FILE_FILENAME_CONFIG,
                temporaryFolder.newFolder().toPath().resolve("offsets").toString());
        props.setProperty(AsyncEngineConfig.TASK_MANAGEMENT_TIMEOUT_MS.name(), "200");
        props.putAll(overrides);
        return new AsyncEmbeddedEngine.AsyncEngineBuilder<SourceRecord>()
                .using(props).using(callback).notifying(consumer).build();
    }
}
