package com.github.fridujo.rabbitmq.mock.metrics;

import com.github.fridujo.rabbitmq.mock.MockConnectionFactory;
import org.junit.jupiter.api.DynamicTest;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.TestFactory;

import java.net.URL;
import java.net.URLClassLoader;
import java.util.Arrays;
import java.util.List;

import static com.github.fridujo.rabbitmq.mock.tool.SafeArgumentMatchers.eq;
import static org.assertj.core.api.Assertions.assertThat;
import static org.junit.jupiter.api.DynamicTest.dynamicTest;
import static org.mockito.Mockito.doThrow;
import static org.mockito.Mockito.spy;

class MetricsCollectorWrapperTest {

    @Test
    void current_version_resolve_MetricsCollector() {
        MetricsCollectorWrapper metricsCollectorWrapper = MetricsCollectorWrapper.Builder.build(new MockConnectionFactory());

        assertThat(metricsCollectorWrapper).isExactlyInstanceOf(ImplementedMetricsCollectorWrapper.class);
    }

    @Test
    void unresolved_MetricsCollector_leads_to_noop_implementation() throws ClassNotFoundException {
        ClassLoader classLoader = spy(new URLClassLoader(new URL[0], this.getClass().getClassLoader()));
        doThrow(new ClassNotFoundException()).when(classLoader).loadClass(eq("com.rabbitmq.client.MetricsCollector"));
        MetricsCollectorWrapper metricsCollectorWrapper = MetricsCollectorWrapper.Builder.build(classLoader, new MockConnectionFactory());

        assertThat(metricsCollectorWrapper).isExactlyInstanceOf(NoopMetricsCollectorWrapper.class);
    }

    @TestFactory
    List<DynamicTest> noop_implementation_never_throws() {
        MetricsCollectorWrapper metricsCollectorWrapper = new NoopMetricsCollectorWrapper();
        return Arrays.asList(
            dynamicTest("newConnection", () -> metricsCollectorWrapper.newConnection(null)),
            dynamicTest("closeConnection", () -> metricsCollectorWrapper.closeConnection(null)),
            dynamicTest("newChannel", () -> metricsCollectorWrapper.newChannel(null)),
            dynamicTest("closeChannel", () -> metricsCollectorWrapper.closeChannel(null)),
            dynamicTest("basicPublish", () -> metricsCollectorWrapper.basicPublish(null)),
            dynamicTest("consumedMessage", () -> metricsCollectorWrapper.consumedMessage(null, 0L, true)),
            dynamicTest("consumedMessage (consumerTag)", () -> metricsCollectorWrapper.consumedMessage(null, 0L, null)),
            dynamicTest("basicAck", () -> metricsCollectorWrapper.basicAck(null, 0L, true)),
            dynamicTest("basicNack", () -> metricsCollectorWrapper.basicNack(null, 0L)),
            dynamicTest("basicReject", () -> metricsCollectorWrapper.basicReject(null, 0L)),
            dynamicTest("basicConsume", () -> metricsCollectorWrapper.basicConsume(null, null, true)),
            dynamicTest("basicCancel", () -> metricsCollectorWrapper.basicCancel(null, null))
        );
    }
}
