package com.github.fridujo.rabbitmq.mock;

import com.github.fridujo.rabbitmq.mock.metrics.MetricsCollectorWrapper;
import com.rabbitmq.client.AddressResolver;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Collection;
import java.util.HashMap;
import java.util.HashSet;
import java.util.concurrent.ExecutorService;

import static java.lang.String.format;

public class MockConnectionFactory extends ConfigurableConnectionFactory<MockConnectionFactory> {

    private static final Logger LOGGER = LoggerFactory.getLogger(MockConnectionFactory.class);

    private HashMap<String, MockPolicy> policies = new HashMap<>();

    public MockConnectionFactory() {
        setAutomaticRecoveryEnabled(false);
    }

    @Override
    public MockConnection newConnection(ExecutorService executor, AddressResolver addressResolver, String clientProvidedName) {
        return newConnection();
    }

    public MockConnection newConnection() {
        MetricsCollectorWrapper metricsCollectorWrapper = MetricsCollectorWrapper.Builder.build(this);
        MockConnection mockConnection = new MockConnection(mockNode, metricsCollectorWrapper);
        return mockConnection;
    }

    public void setPolicy(MockPolicy policy) {
        policies.put(policy.getName(), policy);
        mockNode.applyPolicies(new HashSet(policies.values()));
    }

    public void deletePolicy(String policyName) {
        if(policies.remove(policyName) == null) {
            LOGGER.error(format("Error deleting, policy with name %s was not found", policyName));
        } else {
            mockNode.applyPolicies(new HashSet(policies.values()));
        }
    }

    public Collection<MockPolicy> listPolicies() {
        return policies.values();
    }

}
