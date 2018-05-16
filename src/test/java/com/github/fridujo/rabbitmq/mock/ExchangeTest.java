package com.github.fridujo.rabbitmq.mock;

import org.junit.jupiter.api.Nested;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.CsvSource;

import static org.assertj.core.api.Assertions.assertThat;

class ExchangeTest {

    @Nested
    class DirectTest {
        @ParameterizedTest(name = "{1} matches {0} as direct bindingKey")
        @CsvSource({
            "some.key, some.key",
            "some.other.key, some.other.key"
        })
        void binding_key_matches_routing_key(String bindingKey, String routingKey) {
            MockTopicExchange topicExchange = new MockTopicExchange("test");

            assertThat(topicExchange.match(bindingKey, routingKey)).isTrue();
        }

        @ParameterizedTest(name = "{1} does not match {0} as direct bindingKey")
        @CsvSource({
            "some.key, other.key",
            "*.orange.*, quick.orange.rabbit",
            "lazy.#, lazy.pink.rabbit"
        })
        void binding_key_does_not_match_routing_key(String bindingKey, String routingKey) {
            MockDirectExchange topicExchange = new MockDirectExchange("test");

            assertThat(topicExchange.match(bindingKey, routingKey)).isFalse();
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
            MockTopicExchange topicExchange = new MockTopicExchange("test");

            assertThat(topicExchange.match(bindingKey, routingKey)).isTrue();
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
            MockTopicExchange topicExchange = new MockTopicExchange("test");

            assertThat(topicExchange.match(bindingKey, routingKey)).isFalse();
        }
    }
}
