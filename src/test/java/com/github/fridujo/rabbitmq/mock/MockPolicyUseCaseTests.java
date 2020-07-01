package com.github.fridujo.rabbitmq.mock;

import com.rabbitmq.client.AMQP;
import com.rabbitmq.client.BuiltinExchangeType;
import com.rabbitmq.client.Channel;
import com.rabbitmq.client.Connection;
import com.rabbitmq.client.DefaultConsumer;
import com.rabbitmq.client.Envelope;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

import java.io.IOException;
import java.time.Duration;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.concurrent.TimeoutException;

import static com.github.fridujo.rabbitmq.mock.MockPolicy.ApplyTo.EXCHANGE;
import static com.github.fridujo.rabbitmq.mock.MockPolicy.ApplyTo.QUEUE;
import static java.lang.Thread.sleep;
import static org.assertj.core.api.Assertions.assertThat;

class MockPolicyUseCaseTests {

    public static final long TIMEOUT_INTERVAL = 200L;
    public static final long RETRY_INTERVAL = 10L;

    @Test
    void canApplyDeadLetterExchangeAsPolicy() throws IOException, TimeoutException {
        MockConnectionFactory connectionFactory = new MockConnectionFactory();
        try (Connection conn = connectionFactory.newConnection()) {
            try (Channel channel = conn.createChannel()) {
                channel.exchangeDeclare("rejected-ex", BuiltinExchangeType.FANOUT);
                channel.queueDeclare("rejected", true, false, false, null);
                channel.queueBind("rejected", "rejected-ex", "", null);
                channel.queueDeclare("data", true, false, false, null);

                MockPolicy deadLetterExchangePolicy = MockPolicy.builder()
                    .name("dead letter exchange policy")
                    .pattern("data")
                    .definition(MockPolicy.DEAD_LETTER_EXCHANGE, "rejected-ex")
                    .build();

                connectionFactory.setPolicy(deadLetterExchangePolicy);

                List<byte[]> messages = new ArrayList<>();

                configureRejectOnQueue(channel, "data");

                configureMessageCapture(channel, "rejected", messages);

                channel.basicPublish("", "data", null, "bytes".getBytes());

                waitForMessageDelivered(messages);

                assertThat(messages.get(0)).isEqualTo("bytes".getBytes());
            }
        }
    }

    @Test
    void applyingDeadExchangeAsPolicyDoesNotSupersedeQueueArgument() throws IOException, TimeoutException {
        MockConnectionFactory connectionFactory = new MockConnectionFactory();
        try (Connection conn = connectionFactory.newConnection()) {
            try (Channel channel = conn.createChannel()) {
                channel.exchangeDeclare("rejected-ex", BuiltinExchangeType.FANOUT);
                channel.queueDeclare("rejected", true, false, false, null);
                channel.queueBind("rejected", "rejected-ex", "", null);

                channel.exchangeDeclare("rejected-ex", BuiltinExchangeType.FANOUT);
                channel.queueDeclare("rejected", true, false, false, null);
                channel.queueBind("rejected", "rejected-ex", "", null);

                channel.queueDeclare("data", true, false, false, new HashMap<String, Object>() {{ put(AmqArguments.DEAD_LETTER_EXCHANGE_KEY, "rejected-ex"); }});

                MockPolicy deadLetterExchangePolicy = MockPolicy.builder()
                    .name("dead letter exchange policy that does not override queue arguments")
                    .pattern("data")
                    .definition(MockPolicy.DEAD_LETTER_EXCHANGE, "rejected-ex-policy")
                    .build();

                connectionFactory.setPolicy(deadLetterExchangePolicy);

                List<byte[]> messages = new ArrayList<>();

                configureRejectOnQueue(channel, "data");

                configureMessageCapture(channel, "rejected", messages);

                channel.basicPublish("", "data", null, "bytes".getBytes());

                waitForMessageDelivered(messages);

                assertThat(messages.get(0)).isEqualTo("bytes".getBytes());
            }
        }
    }

    @Test
    void canApplyDeadLetterRoutingKeyAsPolicy() throws IOException, TimeoutException {
        MockConnectionFactory connectionFactory = new MockConnectionFactory();
        try (Connection conn = connectionFactory.newConnection()) {
            try (Channel channel = conn.createChannel()) {
                channel.exchangeDeclare("rejected-ex", BuiltinExchangeType.DIRECT);
                channel.queueDeclare("rejected", true, false, false, null);
                channel.queueBind("rejected", "rejected-ex", "routed-by-policy", null);
                channel.queueDeclare("data", true, false, false, new HashMap<String, Object>() {{ put(AmqArguments.DEAD_LETTER_EXCHANGE_KEY, "rejected-ex"); }});

                MockPolicy deadLetterRoutingKeyPolicy = MockPolicy.builder()
                    .name("dead letter routing key policy")
                    .pattern("data")
                    .definition(MockPolicy.DEAD_LETTER_ROUTING_KEY, "routed-by-policy")
                    .build();

                connectionFactory.setPolicy(deadLetterRoutingKeyPolicy);

                List<byte[]> messages = new ArrayList<>();

                configureRejectOnQueue(channel, "data");

                configureMessageCapture(channel, "rejected", messages);

                channel.basicPublish("", "data", null, "bytes".getBytes());

                waitForMessageDelivered(messages);

                assertThat(messages.get(0)).isEqualTo("bytes".getBytes());
            }
        }
    }

    @Test
    void applyingDeadLetterRoutingKeyAsPolicyDoesNotSupersedeQueueArguments() throws IOException, TimeoutException {
        MockConnectionFactory connectionFactory = new MockConnectionFactory();
        try (Connection conn = connectionFactory.newConnection()) {
            try (Channel channel = conn.createChannel()) {
                channel.exchangeDeclare("rejected-ex", BuiltinExchangeType.DIRECT);
                channel.queueDeclare("rejected", true, false, false, null);
                channel.queueBind("rejected", "rejected-ex", "routed-by-queue-args", null);
                channel.queueDeclare("data", true, false, false,
                    new HashMap<String, Object>() {{
                        put(AmqArguments.DEAD_LETTER_EXCHANGE_KEY, "rejected-ex");
                        put(AmqArguments.DEAD_LETTER_ROUTING_KEY_KEY, "routed-by-queue-args");
                    }});

                MockPolicy deadLetterRoutingKeyPolicy = MockPolicy.builder()
                    .name("dead letter routing key policy that does not override queue arguments")
                    .pattern("data")
                    .definition(MockPolicy.DEAD_LETTER_ROUTING_KEY, "routed-by-policy")
                    .build();

                connectionFactory.setPolicy(deadLetterRoutingKeyPolicy);

                List<byte[]> messages = new ArrayList<>();

                configureRejectOnQueue(channel, "data");

                configureMessageCapture(channel, "rejected", messages);

                channel.basicPublish("", "data", null, "bytes".getBytes());

                waitForMessageDelivered(messages);

                assertThat(messages.get(0)).isEqualTo("bytes".getBytes());
            }
        }
    }

    @Test
    void canApplyAlternativeExchangeAsPolicy() throws IOException, TimeoutException {
        MockConnectionFactory connectionFactory = new MockConnectionFactory();
        try (Connection conn = connectionFactory.newConnection()) {
            try (Channel channel = conn.createChannel()) {
                channel.exchangeDeclare("non-routable-ex", BuiltinExchangeType.FANOUT);
                channel.exchangeDeclare("alternative-ex", BuiltinExchangeType.FANOUT);
                channel.queueDeclare("alternative", true, false, false, null);
                channel.queueBind("alternative", "alternative-ex", "", null);

                MockPolicy alternativeExchangePolicy = MockPolicy.builder()
                    .name("alternative exchange policy")
                    .pattern("non-routable-ex")
                    .definition(MockPolicy.ALTERNATE_EXCHANGE, "alternative-ex")
                    .build();

                connectionFactory.setPolicy(alternativeExchangePolicy);

                List<byte[]> messages = new ArrayList<>();

                configureMessageCapture(channel, "alternative", messages);

                channel.basicPublish("non-routable-ex", "", null, "bytes".getBytes());

                waitForMessageDelivered(messages);

                assertThat(messages.get(0)).isEqualTo("bytes".getBytes());
            }
        }
    }

    @Test
    void applyingAlternativeExchangeAsPolicyDoesNotSupersedeExchangeArguments() throws IOException, TimeoutException {
        MockConnectionFactory connectionFactory = new MockConnectionFactory();
        try (Connection conn = connectionFactory.newConnection()) {
            try (Channel channel = conn.createChannel()) {
                channel.exchangeDeclare("non-routable-ex", BuiltinExchangeType.FANOUT, true, false,
                    new HashMap<String, Object>() {{
                        put(AmqArguments.ALTERNATE_EXCHANGE_KEY, "alternative-ex");
                    }});
                channel.exchangeDeclare("alternative-ex", BuiltinExchangeType.FANOUT);
                channel.queueDeclare("alternative", true, false, false, null);
                channel.queueBind("alternative", "alternative-ex", "", null);

                MockPolicy alternativeExchangePolicy = MockPolicy.builder()
                    .name("alternative exchange policy")
                    .pattern("non-routable-ex")
                    .definition(MockPolicy.ALTERNATE_EXCHANGE, "alternative-ex-policy")
                    .build();

                connectionFactory.setPolicy(alternativeExchangePolicy);

                List<byte[]> messages = new ArrayList<>();

                configureMessageCapture(channel, "alternative", messages);

                channel.basicPublish("non-routable-ex", "", null, "bytes".getBytes());

                waitForMessageDelivered(messages);

                assertThat(messages.get(0)).isEqualTo("bytes".getBytes());
            }
        }
    }

    @Test
    void doesNotApplyPolicyWhenPatternDoesNotMatch() throws IOException, TimeoutException, InterruptedException {
        MockConnectionFactory connectionFactory = new MockConnectionFactory();
        try (Connection conn = connectionFactory.newConnection()) {
            try (Channel channel = conn.createChannel()) {
                channel.exchangeDeclare("non-routable-ex", BuiltinExchangeType.FANOUT);
                channel.exchangeDeclare("alternative-ex", BuiltinExchangeType.FANOUT);
                channel.queueDeclare("alternative", true, false, false, null);
                channel.queueBind("alternative", "alternative-ex", "", null);

                MockPolicy alternativeExchangePolicy = MockPolicy.builder()
                    .name("alternative exchange policy")
                    .pattern("does-not-match-non-routable-ex")
                    .definition(MockPolicy.ALTERNATE_EXCHANGE, "alternative-ex")
                    .build();

                connectionFactory.setPolicy(alternativeExchangePolicy);

                List<byte[]> messages = new ArrayList<>();

                configureMessageCapture(channel, "alternative", messages);

                channel.basicPublish("non-routable-ex", "", null, "bytes".getBytes());

                sleep(TIMEOUT_INTERVAL);

                assertThat(messages).isEmpty();
            }
        }
    }

    @Test
    void canUseRegExAsPatternToApplyPolicy() throws IOException, TimeoutException {
        MockConnectionFactory connectionFactory = new MockConnectionFactory();
        try (Connection conn = connectionFactory.newConnection()) {
            try (Channel channel = conn.createChannel()) {
                channel.exchangeDeclare("non-routable-ex", BuiltinExchangeType.FANOUT);
                channel.exchangeDeclare("alternative-ex", BuiltinExchangeType.FANOUT);
                channel.queueDeclare("alternative", true, false, false, null);
                channel.queueBind("alternative", "alternative-ex", "", null);

                MockPolicy alternativeExchangePolicy = MockPolicy.builder()
                    .name("alternative exchange policy")
                    .pattern(".*routable-ex$")
                    .definition(MockPolicy.ALTERNATE_EXCHANGE, "alternative-ex")
                    .build();

                connectionFactory.setPolicy(alternativeExchangePolicy);

                List<byte[]> messages = new ArrayList<>();

                configureMessageCapture(channel, "alternative", messages);

                channel.basicPublish("non-routable-ex", "", null, "bytes".getBytes());

                waitForMessageDelivered(messages);

                assertThat(messages.get(0)).isEqualTo("bytes".getBytes());
            }
        }
    }

    @Test
    void highestPriorityPolicyIsApplied() throws IOException, TimeoutException {
        MockConnectionFactory connectionFactory = new MockConnectionFactory();
        try (Connection conn = connectionFactory.newConnection()) {
            try (Channel channel = conn.createChannel()) {
                channel.exchangeDeclare("non-routable-ex", BuiltinExchangeType.FANOUT);
                channel.exchangeDeclare("alternative-ex", BuiltinExchangeType.FANOUT);
                channel.exchangeDeclare("unroutable-alternative-ex", BuiltinExchangeType.FANOUT);
                channel.queueDeclare("alternative", true, false, false, null);
                channel.queueBind("alternative", "alternative-ex", "", null);

                MockPolicy lowPriorityAlternativeExchangePolicy = MockPolicy.builder()
                    .name(" low priority alternative exchange policy")
                    .pattern("non-routable-ex")
                    .definition(MockPolicy.ALTERNATE_EXCHANGE, "unroutable-alternative-ex")
                    .build();

                MockPolicy highPriorityAlternativeExchangePolicy = MockPolicy.builder()
                    .name("high priority alternative exchange policy")
                    .pattern("non-routable-ex")
                    .definition(MockPolicy.ALTERNATE_EXCHANGE, "alternative-ex")
                    .priority(1)
                    .build();

                connectionFactory.setPolicy(lowPriorityAlternativeExchangePolicy);
                connectionFactory.setPolicy(highPriorityAlternativeExchangePolicy);

                List<byte[]> messages = new ArrayList<>();

                configureMessageCapture(channel, "alternative", messages);

                channel.basicPublish("non-routable-ex", "", null, "bytes".getBytes());

                waitForMessageDelivered(messages);

                assertThat(messages.get(0)).isEqualTo("bytes".getBytes());
            }
        }
    }

    @Test
    void appliesSinglePolicyWhenMultiplePatternsMatchWithSamePriority() throws IOException, TimeoutException {
        MockConnectionFactory connectionFactory = new MockConnectionFactory();
        try (Connection conn = connectionFactory.newConnection()) {
            try (Channel channel = conn.createChannel()) {
                channel.exchangeDeclare("non-routable-ex", BuiltinExchangeType.FANOUT);
                channel.exchangeDeclare("alternative-ex", BuiltinExchangeType.FANOUT);
                channel.exchangeDeclare("unroutable-alternative-ex", BuiltinExchangeType.FANOUT);
                channel.queueDeclare("alternative", true, false, false, null);
                channel.queueBind("alternative", "alternative-ex", "", null);

                MockPolicy policy = MockPolicy.builder()
                    .name("one")
                    .pattern("non-routable-ex")
                    .definition(MockPolicy.ALTERNATE_EXCHANGE, "alternative-ex")
                    .build();

                connectionFactory.setPolicy(policy);
                connectionFactory.setPolicy(policy.toBuilder().name("three").build());
                connectionFactory.setPolicy(policy.toBuilder().name("two").build());

                List<byte[]> messages = new ArrayList<>();

                configureMessageCapture(channel, "alternative", messages);

                channel.basicPublish("non-routable-ex", "", null, "bytes".getBytes());

                waitForMessageDelivered(messages);

                assertThat(messages.get(0)).isEqualTo("bytes".getBytes());
            }
        }
    }

    @Test
    void queuePolicyIsNotAppliedWhenAppliesToSetToExchanges() throws IOException, TimeoutException, InterruptedException {
        MockConnectionFactory connectionFactory = new MockConnectionFactory();
        try (Connection conn = connectionFactory.newConnection()) {
            try (Channel channel = conn.createChannel()) {
                channel.exchangeDeclare("rejected-ex", BuiltinExchangeType.FANOUT);
                channel.queueDeclare("rejected", true, false, false, null);
                channel.queueBind("rejected", "rejected-ex", "", null);
                channel.queueDeclare("data", true, false, false, null);

                MockPolicy deadLetterExchangePolicy = MockPolicy.builder()
                    .name("dead letter exchange policy")
                    .pattern("data")
                    .definition(MockPolicy.DEAD_LETTER_EXCHANGE, "rejected-ex")
                    .applyTo(EXCHANGE)
                    .build();

                connectionFactory.setPolicy(deadLetterExchangePolicy);

                List<byte[]> messages = new ArrayList<>();

                configureRejectOnQueue(channel, "data");

                configureMessageCapture(channel, "rejected", messages);

                channel.basicPublish("", "data", null, "bytes".getBytes());

                sleep(TIMEOUT_INTERVAL);

                assertThat(messages).isEmpty();
            }
        }
    }

    @Test
    void cannotApplyPolicyToDefaultExchange() throws IOException, TimeoutException, InterruptedException {
        MockConnectionFactory connectionFactory = new MockConnectionFactory();
        try (Connection conn = connectionFactory.newConnection()) {
            try (Channel channel = conn.createChannel()) {
                channel.exchangeDeclare("non-routable-ex", BuiltinExchangeType.FANOUT);
                channel.exchangeDeclare("alternative-ex", BuiltinExchangeType.FANOUT);
                channel.queueDeclare("alternative", true, false, false, null);
                channel.queueBind("alternative", "alternative-ex", "", null);

                MockPolicy alternativeExchangePolicy = MockPolicy.builder()
                    .name("alternative exchange policy")
                    .pattern("")
                    .definition(MockPolicy.ALTERNATE_EXCHANGE, "alternative-ex")
                    .build();

                connectionFactory.setPolicy(alternativeExchangePolicy);

                List<byte[]> messages = new ArrayList<>();

                configureMessageCapture(channel, "alternative", messages);

                channel.basicPublish("", "non-routable-ex", null, "bytes".getBytes());

                sleep(TIMEOUT_INTERVAL);

                assertThat(messages).isEmpty();
            }
        }
    }

    @Test
    void exchangePolicyIsNotAppliedWhenAppliesToSetToQueues() throws IOException, TimeoutException, InterruptedException {
        MockConnectionFactory connectionFactory = new MockConnectionFactory();
        try (Connection conn = connectionFactory.newConnection()) {
            try (Channel channel = conn.createChannel()) {
                channel.exchangeDeclare("non-routable-ex", BuiltinExchangeType.FANOUT);
                channel.exchangeDeclare("alternative-ex", BuiltinExchangeType.FANOUT);
                channel.queueDeclare("alternative", true, false, false, null);
                channel.queueBind("alternative", "alternative-ex", "", null);

                MockPolicy alternativeExchangePolicy = MockPolicy.builder()
                    .name("alternative exchange policy")
                    .pattern("non-routable-ex")
                    .definition(MockPolicy.ALTERNATE_EXCHANGE, "alternative-ex")
                    .applyTo(QUEUE)
                    .build();

                connectionFactory.setPolicy(alternativeExchangePolicy);

                List<byte[]> messages = new ArrayList<>();

                configureMessageCapture(channel, "alternative", messages);

                channel.basicPublish("non-routable-ex", "", null, "bytes".getBytes());

                sleep(TIMEOUT_INTERVAL);

                assertThat(messages).isEmpty();
            }
        }
    }

    @Test
    void policiesCanBeChangedDynamicallyWithSetAndDelete() throws IOException, TimeoutException {
        MockConnectionFactory connectionFactory = new MockConnectionFactory();
        try (Connection conn = connectionFactory.newConnection()) {
            try (Channel channel = conn.createChannel()) {
                channel.exchangeDeclare("rejected-ex", BuiltinExchangeType.FANOUT);
                channel.exchangeDeclare("rejected-ex-2", BuiltinExchangeType.FANOUT);
                channel.queueDeclare("rejected", true, false, false, null);
                channel.queueDeclare("rejected-2", true, false, false, null);
                channel.queueBind("rejected", "rejected-ex", "", null);
                channel.queueBind("rejected-2", "rejected-ex-2", "", null);
                channel.queueDeclare("data", true, false, false, null);

                List<byte[]> messagesFromPolicyA = new ArrayList<>();
                List<byte[]> messagesFromPolicyB = new ArrayList<>();

                MockPolicy policyA = MockPolicy.builder()
                    .name("dead letter exchange policy")
                    .pattern("data")
                    .definition(MockPolicy.DEAD_LETTER_EXCHANGE, "rejected-ex")
                    .build();

                connectionFactory.setPolicy(policyA);
                assertThat(connectionFactory.listPolicies()).containsExactly(policyA);

                configureRejectOnQueue(channel, "data");
                configureMessageCapture(channel, "rejected", messagesFromPolicyA);
                configureMessageCapture(channel, "rejected-2", messagesFromPolicyB);

                channel.basicPublish("", "data", null, "bytes".getBytes());

                waitForMessageDelivered(messagesFromPolicyA);

                assertThat(messagesFromPolicyA.get(0)).isEqualTo("bytes".getBytes());
                assertThat(messagesFromPolicyB).isEmpty();

                messagesFromPolicyA.clear();

                MockPolicy policyB = MockPolicy.builder()
                    .name("higher priority dead letter exchange policy")
                    .pattern("data")
                    .definition(MockPolicy.DEAD_LETTER_EXCHANGE, "rejected-ex-2")
                    .priority(1)
                    .build();

                connectionFactory.setPolicy(policyB);
                assertThat(connectionFactory.listPolicies()).containsExactly(policyA, policyB);

                channel.basicPublish("", "data", null, "bytes".getBytes());

                waitForMessageDelivered(messagesFromPolicyB);

                assertThat(messagesFromPolicyB.get(0)).isEqualTo("bytes".getBytes());
                assertThat(messagesFromPolicyA).isEmpty();

                messagesFromPolicyB.clear();


                MockPolicy higherPriorityPolicyA = policyA.toBuilder().priority(2).build();
                connectionFactory.setPolicy(higherPriorityPolicyA);
                assertThat(connectionFactory.listPolicies()).containsExactly(higherPriorityPolicyA, policyB);


                channel.basicPublish("", "data", null, "bytes".getBytes());

                waitForMessageDelivered(messagesFromPolicyA);

                assertThat(messagesFromPolicyA.get(0)).isEqualTo("bytes".getBytes());
                assertThat(messagesFromPolicyB).isEmpty();

                messagesFromPolicyA.clear();

                connectionFactory.deletePolicy(policyA.getName());
                assertThat(connectionFactory.listPolicies()).containsExactly(policyB);

                channel.basicPublish("", "data", null, "bytes".getBytes());

                waitForMessageDelivered(messagesFromPolicyB);

                assertThat(messagesFromPolicyB.get(0)).isEqualTo("bytes".getBytes());
                assertThat(messagesFromPolicyA).isEmpty();
            }
        }
    }


    private void configureRejectOnQueue(Channel channel, String queueName) throws IOException {
        channel.basicConsume(queueName, new DefaultConsumer(channel) {
            @Override
            public void handleDelivery(String consumerTag, Envelope envelope, AMQP.BasicProperties properties, byte[] body) throws IOException {
                channel.basicReject(envelope.getDeliveryTag(), false);
            }
        });
    }

    private void configureMessageCapture(Channel channel, String queueName, List messages) throws IOException {
        channel.basicConsume(queueName, new DefaultConsumer(channel) {
            @Override
            public void handleDelivery(String consumerTag, Envelope envelope, AMQP.BasicProperties properties, byte[] body) {
                messages.add(body);
            }
        });
    }

    private void waitForMessageDelivered(List messages) {
        Assertions.assertTimeoutPreemptively(Duration.ofMillis(TIMEOUT_INTERVAL), () -> {
            while(messages.isEmpty()) {
                sleep(RETRY_INTERVAL);
            }
        });
    }
}
