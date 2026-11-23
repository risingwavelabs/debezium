/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.debezium.embedded.async;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

import java.util.List;
import java.util.Properties;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicReference;

import org.apache.kafka.connect.connector.Task;
import org.apache.kafka.connect.runtime.ConnectorConfig;
import org.apache.kafka.connect.runtime.standalone.StandaloneConfig;
import org.apache.kafka.connect.source.SourceRecord;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TemporaryFolder;

import io.debezium.connector.simple.SimpleSourceConnector;
import io.debezium.engine.DebeziumEngine;

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

    private DebeziumEngine<SourceRecord> build(Class<?> connector, DebeziumEngine.CompletionCallback callback) throws Exception {
        Properties props = new Properties();
        props.setProperty(ConnectorConfig.NAME_CONFIG, "review-37");
        props.setProperty(ConnectorConfig.TASKS_MAX_CONFIG, "1");
        props.setProperty(ConnectorConfig.CONNECTOR_CLASS_CONFIG, connector.getName());
        props.setProperty(StandaloneConfig.OFFSET_STORAGE_FILE_FILENAME_CONFIG,
                temporaryFolder.newFolder().toPath().resolve("offsets").toString());
        props.setProperty(AsyncEngineConfig.TASK_MANAGEMENT_TIMEOUT_MS.name(), "200");
        return new AsyncEmbeddedEngine.AsyncEngineBuilder<SourceRecord>()
                .using(props).using(callback).notifying((records, committer) -> {
                }).build();
    }
}
