package com.github.fridujo.rabbitmq.mock.exchange;

import com.github.fridujo.rabbitmq.mock.AmqArguments;
import com.github.fridujo.rabbitmq.mock.ReceiverRegistry;
import com.rabbitmq.client.BuiltinExchangeType;
import org.junit.jupiter.api.Nested;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.CsvSource;

import java.util.LinkedHashMap;
import java.util.Map;

import static java.util.Collections.emptyMap;
import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatExceptionOfType;
import static org.mockito.Mockito.mock;

class ExchangeTest {

    private static AmqArguments empty() {
        return new AmqArguments(emptyMap());
    }

    @Test
    void mockExchangeFactiry_throws_if_type_is_unknown() {
        assertThatExceptionOfType(IllegalArgumentException.class)
            .isThrownBy(() -> MockExchangeFactory.build("test", "unknown type", empty(), mock(ReceiverRegistry.class)))
            .withMessage("No exchange type unknown type");
    }

    @Nested
    class DirectTest {
        @ParameterizedTest(name = "{1} matches {0} as direct bindingKey")
        @CsvSource({
            "some.key, some.key",
            "some.other.key, some.other.key"
        })
        void binding_key_matches_routing_key(String bindingKey, String routingKey) {
            BindableMockExchange directExchange = MockExchangeFactory.build("test", BuiltinExchangeType.DIRECT.getType(), empty(), mock(ReceiverRegistry.class));

            assertThat(directExchange.match(bindingKey, emptyMap(), routingKey, emptyMap())).isTrue();
        }

        @ParameterizedTest(name = "{1} does not match {0} as direct bindingKey")
        @CsvSource({
            "some.key, other.key",
            "*.orange.*, quick.orange.rabbit",
            "lazy.#, lazy.pink.rabbit"
        })
        void binding_key_does_not_match_routing_key(String bindingKey, String routingKey) {
            BindableMockExchange directExchange = MockExchangeFactory.build("test", BuiltinExchangeType.DIRECT.getType(), empty(), mock(ReceiverRegistry.class));

            assertThat(directExchange.match(bindingKey, emptyMap(), routingKey, emptyMap())).isFalse();
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
            BindableMockExchange fanoutExchange = MockExchangeFactory.build("test", BuiltinExchangeType.FANOUT.getType(), empty(), mock(ReceiverRegistry.class));

            assertThat(fanoutExchange.match(bindingKey, emptyMap(), routingKey, emptyMap())).isTrue();
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
            BindableMockExchange topicExchange = MockExchangeFactory.build("test", BuiltinExchangeType.TOPIC.getType(), empty(), mock(ReceiverRegistry.class));

            assertThat(topicExchange.match(bindingKey, emptyMap(), routingKey, emptyMap())).isTrue();
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
            BindableMockExchange topicExchange = MockExchangeFactory.build("test", BuiltinExchangeType.TOPIC.getType(), empty(), mock(ReceiverRegistry.class));

            assertThat(topicExchange.match(bindingKey, emptyMap(), routingKey, emptyMap())).isFalse();
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
            BindableMockExchange headersExchange = MockExchangeFactory.build("test", BuiltinExchangeType.HEADERS.getType(), empty(), mock(ReceiverRegistry.class));

            assertThat(headersExchange.match("unused", map("os", "linux", "cores", "8"), "unused", map(headers.split(",\\s*")))).isEqualTo(matches);
        }

        @ParameterizedTest(name = "{0} matching all headers '{'os: 'linux', cores: '8''}': {1}")
        @CsvSource({
            "'', false",
            "'os, linux', false",
            "'os, linux, cores, 4', false",
            "'os, linux, cores, 8', true",
        })
        void headers_topic_with_x_match_all_does_not_match_if_one_header_is_not_matching(String headers, boolean matches) {
            BindableMockExchange headersExchange = MockExchangeFactory.build("test", BuiltinExchangeType.HEADERS.getType(), empty(), mock(ReceiverRegistry.class));

            assertThat(headersExchange.match("unused", map("os", "linux", "cores", "8", "x-match", "all"), "unused", map(headers.split(",\\s*")))).isEqualTo(matches);
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
            BindableMockExchange headersExchange = MockExchangeFactory.build("test", BuiltinExchangeType.HEADERS.getType(), empty(), mock(ReceiverRegistry.class));

            assertThat(headersExchange.match("unused", map("os", "linux", "cores", "8", "x-match", "any"), "unused", map(headers.split(",\\s*")))).isEqualTo(matches);
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
}
