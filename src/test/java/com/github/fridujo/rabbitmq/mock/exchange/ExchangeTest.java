package com.github.fridujo.rabbitmq.mock.exchange;

import static com.github.fridujo.rabbitmq.mock.exchange.MockExchangeCreator.creatorWithExchangeType;
import static java.util.Collections.emptyMap;
import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatExceptionOfType;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import java.util.LinkedHashMap;
import java.util.Map;
import java.util.Optional;

import org.junit.jupiter.api.Nested;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.CsvSource;
import org.mockito.ArgumentCaptor;

import com.github.fridujo.rabbitmq.mock.AmqArguments;
import com.github.fridujo.rabbitmq.mock.MockNode;
import com.github.fridujo.rabbitmq.mock.MockQueue;
import com.github.fridujo.rabbitmq.mock.ReceiverRegistry;
import com.github.fridujo.rabbitmq.mock.configuration.Configuration;
import com.github.fridujo.rabbitmq.mock.exchange.BindableMockExchange.BindConfiguration;
import com.rabbitmq.client.AMQP;
import com.rabbitmq.client.BuiltinExchangeType;

class ExchangeTest {

    private final Configuration configuration = new Configuration();
    private final MockExchangeFactory mockExchangeFactory = new MockExchangeFactory(configuration);

    private static final AmqArguments empty() {
        return new AmqArguments(emptyMap());
    }

    @Test
    void mockExchangeFactory_throws_if_type_is_unknown() {
        assertThatExceptionOfType(IllegalArgumentException.class)
            .isThrownBy(() -> mockExchangeFactory.build("test", "unknown type", empty(), mock(ReceiverRegistry.class)))
            .withMessage("No exchange type unknown type");
    }

    @Test
    void mockExchangeFactory_register_new_mock_exchange() {
        configuration.registerAdditionalExchangeCreator(creatorWithExchangeType(FixDelayExchange.TYPE, FixDelayExchange::new));
        BindableMockExchange xDelayedMockExchange = mockExchangeFactory.build("test", "x-fix-delayed-message", empty(), mock(ReceiverRegistry.class));
        assertThat(xDelayedMockExchange).isExactlyInstanceOf(FixDelayExchange.class);
    }

    @Nested
    class DirectTest {
        @ParameterizedTest(name = "{1} matches {0} as direct bindingKey")
        @CsvSource({
            "some.key, some.key",
            "some.other.key, some.other.key"
        })
        void binding_key_matches_routing_key(String bindingKey, String routingKey) {
            MultipleReceiverExchange directExchange = (MultipleReceiverExchange) mockExchangeFactory.build("test", BuiltinExchangeType.DIRECT.getType(), empty(), mock(ReceiverRegistry.class));
            BindConfiguration bindConfiguration = new BindConfiguration(bindingKey, null, emptyMap());

            assertThat(directExchange.match(bindConfiguration, routingKey, emptyMap())).isTrue();
        }

        @ParameterizedTest(name = "{1} does not match {0} as direct bindingKey")
        @CsvSource({
            "some.key, other.key",
            "*.orange.*, quick.orange.rabbit",
            "lazy.#, lazy.pink.rabbit"
        })
        void binding_key_does_not_match_routing_key(String bindingKey, String routingKey) {
            MultipleReceiverExchange directExchange = (MultipleReceiverExchange) mockExchangeFactory.build("test", BuiltinExchangeType.DIRECT.getType(), empty(), mock(ReceiverRegistry.class));
            BindConfiguration bindConfiguration = new BindConfiguration(bindingKey, null, emptyMap());

            assertThat(directExchange.match(bindConfiguration, routingKey, emptyMap())).isFalse();
        }
    }

    @Nested
    class FanoutTest {
        @ParameterizedTest(name = "{1} matches {0} as fanout bindingKey")
        @CsvSource({
            "some.key, some.key",
            "some.other.key, some.other.key",
            "some.key, other.key",
            "*.orange.*, quick.orange.rabbit",
            "lazy.#, lazy.pink.rabbit",
            "some.key, other.key",
            "*.orange.*, lazy.pink.rabbit",
            "*.orange.*, quick.orange.male.rabbit",
            "*.*.rabbit, quick.orange.fox",
            "*.*.rabbit, quick.orange.male.rabbit",
            "lazy.#, quick.brown.fox"
        })
        void binding_key_matches_routing_key(String bindingKey, String routingKey) {
            MultipleReceiverExchange fanoutExchange = (MultipleReceiverExchange) mockExchangeFactory.build("test", BuiltinExchangeType.FANOUT.getType(), empty(), mock(ReceiverRegistry.class));
            BindConfiguration bindConfiguration = new BindConfiguration(bindingKey, null, emptyMap());

            assertThat(fanoutExchange.match(bindConfiguration, routingKey, emptyMap())).isTrue();
        }
    }

    @Nested
    class TopicTest {

        @ParameterizedTest(name = "{1} matches {0} as topic bindingKey")
        @CsvSource({
            "some.key, some.key",
            "*.orange.*, quick.orange.rabbit",
            "*.*.rabbit, quick.orange.rabbit",
            "lazy.#, lazy.pink.rabbit"
        })
        void binding_key_matches_routing_key(String bindingKey, String routingKey) {
            MultipleReceiverExchange topicExchange = (MultipleReceiverExchange) mockExchangeFactory.build("test", BuiltinExchangeType.TOPIC.getType(), empty(), mock(ReceiverRegistry.class));
            BindConfiguration bindConfiguration = new BindConfiguration(bindingKey, null, emptyMap());

            assertThat(topicExchange.match(bindConfiguration, routingKey, emptyMap())).isTrue();
        }

        @ParameterizedTest(name = "{1} does not match {0} as topic bindingKey")
        @CsvSource({
            "some.key, other.key",
            "*.orange.*, lazy.pink.rabbit",
            "*.orange.*, quick.orange.male.rabbit",
            "*.*.rabbit, quick.orange.fox",
            "*.*.rabbit, quick.orange.male.rabbit",
            "lazy.#, quick.brown.fox"
        })
        void binding_key_does_not_match_routing_key(String bindingKey, String routingKey) {
            MultipleReceiverExchange topicExchange = (MultipleReceiverExchange) mockExchangeFactory.build("test", BuiltinExchangeType.TOPIC.getType(), empty(), mock(ReceiverRegistry.class));
            BindConfiguration bindConfiguration = new BindConfiguration(bindingKey, null, emptyMap());

            assertThat(topicExchange.match(bindConfiguration, routingKey, emptyMap())).isFalse();
        }
    }

    @Nested
    class HeadersTest {

        @ParameterizedTest(name = "{0} matching by default headers '{'os: 'linux', cores: '8''}': {1}")
        @CsvSource({
            "'', false",
            "'os, linux', false",
            "'os, linux, cores, 4', false",
            "'os, linux, cores, 8', true",
        })
        void headers_topic_without_x_match_does_not_match_if_one_header_is_not_matching(String headers, boolean matches) {
            MultipleReceiverExchange headersExchange = (MultipleReceiverExchange) mockExchangeFactory.build("test", BuiltinExchangeType.HEADERS.getType(), empty(), mock(ReceiverRegistry.class));
            BindConfiguration bindConfiguration = new BindConfiguration("unused", null,
                map("os", "linux", "cores", "8"));

            assertThat(headersExchange.match(bindConfiguration, "unused", map(headers.split(",\\s*")))).isEqualTo(matches);
        }

        @ParameterizedTest(name = "{0} matching all headers '{'os: 'linux', cores: '8''}': {1}")
        @CsvSource({
            "'', false",
            "'os, linux', false",
            "'os, linux, cores, 4', false",
            "'os, linux, cores, 8', true",
        })
        void headers_topic_with_x_match_all_does_not_match_if_one_header_is_not_matching(String headers, boolean matches) {
            MultipleReceiverExchange headersExchange = (MultipleReceiverExchange) mockExchangeFactory.build("test", BuiltinExchangeType.HEADERS.getType(), empty(), mock(ReceiverRegistry.class));
            BindConfiguration bindConfiguration = new BindConfiguration("unused", null,
                map("os", "linux", "cores", "8", "x-match", "all"));

            assertThat(headersExchange.match(bindConfiguration, "unused", map(headers.split(",\\s*")))).isEqualTo(matches);
        }

        @ParameterizedTest(name = "{0} matching any headers '{'os: 'linux', cores: '8''}': {1}")
        @CsvSource({
            "'', false",
            "'os, linux', true",
            "'cores, 8', true",
            "'os, linux, cores, 4', true",
            "'os, linux, cores, 8', true",
            "'os, ios, cores, 8', true",
        })
        void headers_topic_with_x_match_any_matches_if_one_header_is_matching(String headers, boolean matches) {
            MultipleReceiverExchange headersExchange = (MultipleReceiverExchange) mockExchangeFactory.build("test", BuiltinExchangeType.HEADERS.getType(), empty(), mock(ReceiverRegistry.class));
            BindConfiguration bindConfiguration = new BindConfiguration("unused", null,
                map("os", "linux", "cores", "8", "x-match", "any"));

            assertThat(headersExchange.match(bindConfiguration, "unused", map(headers.split(",\\s*")))).isEqualTo(matches);
        }

        private Map<String, Object> map(String... keysAndValues) {
            Map<String, Object> map = new LinkedHashMap<>();
            String lastKey = null;
            for (int i = 0; i < keysAndValues.length; i++) {
                if (i % 2 == 0) {
                    lastKey = keysAndValues[i];
                } else {
                    map.put(lastKey, keysAndValues[i]);
                }
            }
            return map;
        }
    }

    @Nested
    class DefaultExchangeTest {

        @Test
        void publish_uses_empty_exchange_name() {
            MockQueue mockQueue = mock(MockQueue.class);
            MockNode mockNode = mock(MockNode.class);
            when(mockNode.getQueue(any())).thenReturn(Optional.of(mockQueue));
            MockDefaultExchange defaultExchange = new MockDefaultExchange(mockNode);

            String previousExchangeName = "ex-previous";
            String routingKey = "rk.test";
            AMQP.BasicProperties props = new AMQP.BasicProperties();
            byte[] body = "test".getBytes();

            defaultExchange.publish(previousExchangeName, routingKey, props, body);

            ArgumentCaptor<String> publishedExchangeName = ArgumentCaptor.forClass(String.class);
            ArgumentCaptor<String> publishedRoutingKey = ArgumentCaptor.forClass(String.class);
            ArgumentCaptor<AMQP.BasicProperties> publishedProps = ArgumentCaptor.forClass(AMQP.BasicProperties.class);
            ArgumentCaptor<byte[]> publishedBody = ArgumentCaptor.forClass(byte[].class);
            verify(
                mockQueue,
                times(1)
            ).publish(
                publishedExchangeName.capture(),
                publishedRoutingKey.capture(),
                publishedProps.capture(),
                publishedBody.capture()
            );

            assertThat(publishedExchangeName.getValue()).isEmpty();
            assertThat(publishedRoutingKey.getValue()).isSameAs(routingKey);
            assertThat(publishedProps.getValue()).isSameAs(props);
            assertThat(publishedBody.getValue()).isSameAs(body);
        }
    }
}
