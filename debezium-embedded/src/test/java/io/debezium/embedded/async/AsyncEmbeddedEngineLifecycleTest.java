/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.debezium.embedded.async;

import static org.assertj.core.api.Assertions.assertThat;
import static org.awaitility.Awaitility.await;

import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;

import org.apache.kafka.connect.connector.Task;
import org.apache.kafka.connect.source.SourceRecord;
import org.apache.kafka.connect.source.SourceTaskContext;
import org.apache.kafka.connect.storage.FileOffsetBackingStore;
import org.junit.After;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TemporaryFolder;

import io.debezium.connector.simple.SimpleSourceConnector;
import io.debezium.embedded.KafkaConnectUtil;

/** Resource ownership across initialization, shutdown, and completion, using real engines. */
public class AsyncEmbeddedEngineLifecycleTest {
    @Rule
    public TemporaryFolder directory = new TemporaryFolder();
    private ExecutorService threads;
    private static CountDownLatch entered;
    private static CountDownLatch release;
    private static CountDownLatch polling;
    private static final AtomicBoolean resourceOpen = new AtomicBoolean();
    private static final AtomicInteger stops = new AtomicInteger();
    private static boolean blockConnectorStart;
    private static boolean blockStoreStart;
    private static boolean blockStoreStop;
    private static boolean failTaskInitialize;
    private static boolean blockConsumer;
    private static boolean closeFromConsumer;
    private static boolean consumerWaitsForTaskStop;
    private static AsyncEmbeddedEngine<?> activeEngine;
    private static boolean failTransformClose;
    private static final AtomicBoolean finalTransformClosed = new AtomicBoolean();

    @Before
    public void reset() {
        entered = new CountDownLatch(1);
        release = new CountDownLatch(1);
        polling = new CountDownLatch(1);
        resourceOpen.set(false);
        stops.set(0);
        blockConnectorStart = blockStoreStart = blockStoreStop = failTaskInitialize = blockConsumer = failTransformClose = closeFromConsumer = false;
        consumerWaitsForTaskStop = false;
        finalTransformClosed.set(false);
        threads = Executors.newSingleThreadExecutor();
    }

    @After
    public void finish() throws Exception {
        release.countDown();
        threads.shutdownNow();
        assertThat(threads.awaitTermination(5, TimeUnit.SECONDS)).isTrue();
    }

    @Test(timeout = 10000)
    public void closeBeforeRunPreventsInitialization() throws Exception {
        var engine = engine(false);
        engine.close();
        assertThat(engine.hasPendingCleanup()).isFalse();
        engine.run();
        assertThat(resourceOpen.get()).isFalse();
        assertThat(stops.get()).isEqualTo(1);
    }

    @Test(timeout = 10000)
    public void connectorInitializationCannotPublishResourcesAfterCleanup() throws Exception {
        blockConnectorStart = true;
        lateInitialization(false);
    }

    @Test(timeout = 10000)
    public void offsetStoreInitializationCannotPublishResourcesAfterCleanup() throws Exception {
        blockStoreStart = true;
        lateInitialization(true);
    }

    private void lateInitialization(boolean store) throws Exception {
        var engine = engine(store);
        var run = threads.submit(engine);
        assertThat(entered.await(5, TimeUnit.SECONDS)).isTrue();
        engine.close();
        assertThat(engine.hasPendingCleanup()).isTrue();
        assertThat(stops.get()).isZero();
        release.countDown();
        run.get(5, TimeUnit.SECONDS);
        await().atMost(5, TimeUnit.SECONDS).until(() -> !engine.hasPendingCleanup());
        assertThat(resourceOpen.get()).isFalse();
        assertThat(stops.get()).isEqualTo(1);
        assertThat(engine.getShutdownFailure()).isNull();
    }

    @Test(timeout = 10000)
    public void initializeFailureStillStopsTheConstructedTask() throws Exception {
        failTaskInitialize = true;
        var engine = engine(false);
        threads.submit(engine).get(5, TimeUnit.SECONDS);
        await().atMost(5, TimeUnit.SECONDS).until(() -> !engine.hasPendingCleanup());
        assertThat(resourceOpen.get()).isFalse();
        assertThat(stops.get()).isEqualTo(1);
    }

    @Test(timeout = 10000)
    public void blockedOffsetStoreStopDoesNotBlockCloseCaller() throws Exception {
        blockStoreStop = true;
        var engine = engine(true);
        var run = threads.submit(engine);
        assertThat(polling.await(5, TimeUnit.SECONDS)).isTrue();
        engine.close();
        assertThat(entered.await(5, TimeUnit.SECONDS)).isTrue();
        assertThat(engine.hasPendingCleanup()).isTrue();
        release.countDown();
        run.get(5, TimeUnit.SECONDS);
        await().atMost(5, TimeUnit.SECONDS).until(() -> !engine.hasPendingCleanup());
        assertThat(resourceOpen.get()).isFalse();
        assertThat(engine.getShutdownFailure()).isNull();
    }

    @Test(timeout = 10000)
    public void inFlightConsumerKeepsSharedResourcesOwned() throws Exception {
        blockConsumer = true;
        var engine = engine(true);
        var run = threads.submit(engine);
        assertThat(entered.await(5, TimeUnit.SECONDS)).isTrue();
        engine.close();
        assertThat(engine.hasPendingCleanup()).isTrue();
        assertThat(stops.get()).isZero();
        // Completion can report pending cleanup even when a callback ignores interruption.
        run.get(5, TimeUnit.SECONDS);
        assertThat(engine.hasPendingCleanup()).isTrue();
        release.countDown();
        await().atMost(5, TimeUnit.SECONDS).until(() -> !engine.hasPendingCleanup());
        assertThat(resourceOpen.get()).isFalse();
    }

    @Test(timeout = 10000)
    public void taskStopCanReleaseAnInFlightRecordConsumer() throws Exception {
        blockConsumer = consumerWaitsForTaskStop = true;
        var engine = engine(true);
        var run = threads.submit(engine);
        assertThat(entered.await(5, TimeUnit.SECONDS)).isTrue();
        engine.close();
        await().atMost(3, TimeUnit.SECONDS).until(() -> !engine.hasPendingCleanup());
        run.get(5, TimeUnit.SECONDS);
        assertThat(resourceOpen.get()).isFalse();
        assertThat(engine.getShutdownFailure()).isNull();
    }

    @Test(timeout = 10000)
    public void failingTransformCloseDoesNotSkipLaterTransforms() throws Exception {
        failTransformClose = true;
        var engine = engine(false);
        var run = threads.submit(engine);
        assertThat(polling.await(5, TimeUnit.SECONDS)).isTrue();
        engine.close();
        run.get(5, TimeUnit.SECONDS);
        assertThat(finalTransformClosed.get()).isTrue();
        assertThat(engine.getShutdownFailure()).hasRootCauseMessage("transform close failed");
        assertThat(engine.hasPendingCleanup()).isFalse();
    }

    @Test(timeout = 10000)
    public void consumerCanRequestShutdownWithoutWaitingForItself() throws Exception {
        closeFromConsumer = true;
        var engine = engine(false);
        var run = threads.submit(engine);
        assertThat(entered.await(5, TimeUnit.SECONDS)).isTrue();
        run.get(5, TimeUnit.SECONDS);
        await().atMost(5, TimeUnit.SECONDS).until(() -> !engine.hasPendingCleanup());
        assertThat(engine.getShutdownFailure()).isNull();
    }

    public static class FailingTransform extends AsyncEmbeddedEngineTest.CloseTestTransform {
        @Override
        public void close() {
            throw new IllegalStateException("transform close failed");
        }
    }

    public static class FinalTransform extends AsyncEmbeddedEngineTest.CloseTestTransform {
        @Override
        public void close() {
            finalTransformClosed.set(true);
        }
    }

    private AsyncEmbeddedEngine<?> engine(boolean store) throws Exception {
        Properties props = new Properties();
        props.setProperty("name", "lifecycle-review");
        props.setProperty("connector.class", Connector.class.getName());
        props.setProperty("offset.storage.file.filename", directory.newFolder().toPath().resolve("offsets").toString());
        props.setProperty(AsyncEngineConfig.TASK_MANAGEMENT_TIMEOUT_MS.name(), closeFromConsumer ? "10000" : "100");
        props.setProperty(AsyncEngineConfig.RECORD_PROCESSING_SHUTDOWN_TIMEOUT_MS.name(), "100");
        if (failTransformClose) {
            props.setProperty("transforms", "first,last");
            props.setProperty("transforms.first.type", FailingTransform.class.getName());
            props.setProperty("transforms.last.type", FinalTransform.class.getName());
        }
        if (store) {
            props.setProperty("offset.storage", Store.class.getName());
        }
        var builder = new AsyncEmbeddedEngine.AsyncEngineBuilder<SourceRecord>();
        builder.using(props);
        if (consumerWaitsForTaskStop) {
            props.setProperty(AsyncEngineConfig.RECORD_PROCESSING_ORDER.name(), "UNORDERED");
            builder.notifying(record -> block());
        }
        else {
            builder.notifying((records, committer) -> {
                if (closeFromConsumer) {
                    try {
                        activeEngine.close();
                        entered.countDown();
                    }
                    catch (java.io.IOException e) {
                        throw new IllegalStateException(e);
                    }
                }
                if (blockConsumer && !records.isEmpty()) {
                    block();
                }
            });
        }
        activeEngine = (AsyncEmbeddedEngine<?>) builder.build();
        return activeEngine;
    }

    private static void block() {
        entered.countDown();
        boolean done = false;
        while (!done) {
            try {
                release.await();
                done = true;
            }
            catch (InterruptedException ignored) {
                // Model a native operation that does not return when its caller is interrupted.
            }
        }
    }

    public static class Connector extends SimpleSourceConnector {
        @Override
        public void start(Map<String, String> props) {
            super.start(props);
            if (blockConnectorStart) {
                block();
                resourceOpen.set(true);
            }
        }

        @Override
        public Class<? extends Task> taskClass() {
            return LifecycleTask.class;
        }

        @Override
        public void stop() {
            if (!blockStoreStart && !blockStoreStop && !failTaskInitialize) {
                stops.incrementAndGet();
                resourceOpen.set(false);
            }
            super.stop();
        }
    }

    public static class LifecycleTask extends DebeziumAsyncEngineTestUtils.NoOpTask {
        private boolean emitted;

        @Override
        public void initialize(SourceTaskContext context) {
            super.initialize(context);
            if (failTaskInitialize) {
                resourceOpen.set(true);
                throw new IllegalStateException("task initialize failed");
            }
        }

        @Override
        public List<SourceRecord> poll() throws InterruptedException {
            polling.countDown();
            if ((blockConsumer || closeFromConsumer) && !emitted) {
                emitted = true;
                return List.of(new SourceRecord(Map.of("partition", 1), Map.of("offset", 1), "test",
                        org.apache.kafka.connect.data.Schema.STRING_SCHEMA, "value"));
            }
            return super.poll();
        }

        @Override
        public void stop() {
            if (consumerWaitsForTaskStop) {
                release.countDown();
            }
            if (failTaskInitialize) {
                stops.incrementAndGet();
                resourceOpen.set(false);
            }
        }
    }

    public static class Store extends FileOffsetBackingStore {
        public Store() {
            super(KafkaConnectUtil.converterForOffsetStore());
        }

        @Override
        public void start() {
            if (blockStoreStart) {
                block();
            }
            super.start();
            resourceOpen.set(true);
        }

        @Override
        public void stop() {
            if (blockStoreStop) {
                block();
            }
            super.stop();
            stops.incrementAndGet();
            resourceOpen.set(false);
        }
    }
}
