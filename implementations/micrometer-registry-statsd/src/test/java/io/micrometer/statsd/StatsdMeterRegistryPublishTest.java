/**
 * Copyright 2017 Pivotal Software, Inc.
 * <p>
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 * <p>
 * https://www.apache.org/licenses/LICENSE-2.0
 * <p>
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package io.micrometer.statsd;

import io.micrometer.core.Issue;
import io.micrometer.core.instrument.Clock;
import io.micrometer.core.instrument.Counter;
import io.netty.handler.logging.LogLevel;
import org.jetbrains.annotations.NotNull;
import org.junit.jupiter.api.Disabled;
import org.junit.jupiter.api.Test;
import reactor.core.publisher.Flux;
import reactor.netty.Connection;
import reactor.netty.udp.UdpServer;

import java.time.Duration;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.stream.IntStream;

import static org.assertj.core.api.Assertions.assertThat;
import static org.awaitility.Awaitility.await;

/**
 * Tests {@link StatsdMeterRegistry} metrics publishing functionality.
 */
class StatsdMeterRegistryPublishTest {

    StatsdMeterRegistry meterRegistry;

    @Test
    void receiveMetricsSuccessfully() throws InterruptedException {
        CountDownLatch latch = new CountDownLatch(3);
        Connection server = startUdpServer(latch, 0);

        final int port = server.address().getPort();

        meterRegistry = new StatsdMeterRegistry(getUnbufferedConfig(port), Clock.SYSTEM);
        startRegistryAndWaitForClient();
        Counter counter = Counter.builder("my.counter").register(meterRegistry);
        counter.increment();
        counter.increment();
        counter.increment();
        assertThat(latch.await(3, TimeUnit.SECONDS)).isTrue();
        meterRegistry.close();

        server.disposeNow();
    }

    @Test
    @Disabled("TODO fix implementation")
    void resumeSendingMetrics_whenServerIntermittentlyFails() throws InterruptedException {
        CountDownLatch latch = new CountDownLatch(1);
        Connection server = startUdpServer(latch, 0);

        final int port = server.address().getPort();

        meterRegistry = new StatsdMeterRegistry(getUnbufferedConfig(port), Clock.SYSTEM);
        startRegistryAndWaitForClient();
        Counter counter = Counter.builder("my.counter").register(meterRegistry);
        counter.increment(1);
        assertThat(latch.await(1, TimeUnit.SECONDS)).isTrue();
        // stop server
        server.disposeNow();
        // client will try to send but server is down
        IntStream.range(2, 5).forEach(counter::increment);
        // TODO wait until processor queue is drained...
        // start server
        latch = new CountDownLatch(3);
        server = startUdpServer(latch, port);
        counter.increment(5);
        counter.increment(6);
        counter.increment(7);
        assertThat(latch.await(1, TimeUnit.SECONDS)).isTrue();
        meterRegistry.close();

        server.disposeNow();
    }

    @Test
    @Issue("#1676")
    void stopAndStartMeterRegistrySendsMetrics() throws InterruptedException {
        CountDownLatch latch = new CountDownLatch(3);
        Connection server = startUdpServer(latch, 0);

        final int port = server.address().getPort();

        meterRegistry = new StatsdMeterRegistry(getUnbufferedConfig(port), Clock.SYSTEM);
        startRegistryAndWaitForClient();
        Counter counter = Counter.builder("my.counter").register(meterRegistry);
        counter.increment();
        await().until(() -> latch.getCount() == 2);
        meterRegistry.stop();
        await().until(this::clientIsDisposed);
        // These increments shouldn't be sent
        IntStream.range(0, 3).forEach(i -> counter.increment());
        startRegistryAndWaitForClient();
        assertThat(latch.getCount()).isEqualTo(2);
        counter.increment();
        counter.increment();
        assertThat(latch.await(1, TimeUnit.SECONDS)).isTrue();
        meterRegistry.close();

        server.disposeNow();
    }

    @Test
    @Issue("#1676")
    void stopAndStartMeterRegistryWithLineSink() throws InterruptedException {
        CountDownLatch latch = new CountDownLatch(3);
        meterRegistry = StatsdMeterRegistry.builder(StatsdConfig.DEFAULT).lineSink(s -> latch.countDown()).build();
        Counter counter = Counter.builder("my.counter").register(meterRegistry);
        counter.increment();
        meterRegistry.stop();
        // These increments shouldn't be processed
        IntStream.range(0, 3).forEach(i -> counter.increment());
        meterRegistry.start();
        assertThat(latch.getCount()).isEqualTo(2);
        counter.increment();
        counter.increment();
        assertThat(latch.await(1, TimeUnit.SECONDS)).isTrue();
        meterRegistry.close();
    }

    private void startRegistryAndWaitForClient() {
        meterRegistry.start();
        // TODO alternatively, configure the processor to buffer when no subscriber is present... previous behavior used to be this, I think?
        await().until(() -> !clientIsDisposed());
    }

    private boolean clientIsDisposed() {
        return meterRegistry.client.get().isDisposed();
    }

    @NotNull
    private Connection startUdpServer(CountDownLatch latch, int port) {
        return UdpServer.create()
                .host("localhost")
                .port(port)
                .handle((in, out) ->
                        in.receive().asString()
                                .flatMap(packet -> {
                                    latch.countDown();
                                    System.out.println(packet);
                                    return Flux.never();
                                }))
                .wiretap("udpserver", LogLevel.INFO)
                .bindNow(Duration.ofSeconds(2));
    }

    @NotNull
    private StatsdConfig getUnbufferedConfig(int port) {
        return new StatsdConfig() {
            @Override
            public String get(String key) {
                return null;
            }

            @Override
            public int port() {
                return port;
            }

            @Override
            public boolean buffered() {
                return false;
            }
        };
    }
}
