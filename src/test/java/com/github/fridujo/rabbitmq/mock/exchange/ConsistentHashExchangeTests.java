package com.github.fridujo.rabbitmq.mock.exchange;

import static com.github.fridujo.rabbitmq.mock.AmqArguments.empty;
import static com.github.fridujo.rabbitmq.mock.exchange.MockExchangeCreator.creatorWithExchangeType;
import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.within;
import static org.mockito.Mockito.mock;

import java.util.Map;
import java.util.Optional;
import java.util.UUID;
import java.util.function.Function;
import java.util.stream.Collectors;
import java.util.stream.IntStream;

import org.junit.jupiter.api.Test;

import com.github.fridujo.rabbitmq.mock.ReceiverPointer;
import com.github.fridujo.rabbitmq.mock.ReceiverRegistry;
import com.github.fridujo.rabbitmq.mock.configuration.Configuration;

class ConsistentHashExchangeTests {

    private final Configuration configuration = new Configuration()
        .registerAdditionalExchangeCreator(creatorWithExchangeType(ConsistentHashExchange.TYPE, ConsistentHashExchange::new));
    private final MockExchangeFactory mockExchangeFactory = new MockExchangeFactory(configuration);

    @Test
    void same_routing_key_dispatch_to_same_queue() {
        SingleReceiverExchange consistentHashEx = (SingleReceiverExchange) mockExchangeFactory.build("test", "x-consistent-hash", empty(), mock(ReceiverRegistry.class));

        consistentHashEx.bind(new ReceiverPointer(ReceiverPointer.Type.QUEUE, "Q1"), "1", Map.of());
        consistentHashEx.bind(new ReceiverPointer(ReceiverPointer.Type.QUEUE, "Q2"), "2", Map.of());

        String firstRoutingKey = UUID.randomUUID().toString();

        ReceiverPointer firstReceiverPointerSelected = consistentHashEx.selectReceiver(firstRoutingKey, null).get();

        for (; ; ) {
            ReceiverPointer receiverPointer = consistentHashEx.selectReceiver(UUID.randomUUID().toString(), null).get();
            if (!receiverPointer.equals(firstReceiverPointerSelected)) {
                break;
            }
        }

        assertThat(consistentHashEx.selectReceiver(firstRoutingKey, null)).contains(firstReceiverPointerSelected);
    }

    @Test
    void dispatch_respects_queue_weight() {
        SingleReceiverExchange consistentHashEx = (SingleReceiverExchange) mockExchangeFactory.build("test", "x-consistent-hash", empty(), mock(ReceiverRegistry.class));

        ReceiverPointer q1 = new ReceiverPointer(ReceiverPointer.Type.QUEUE, "Q1");
        consistentHashEx.bind(q1, "32", Map.of());
        ReceiverPointer q2 = new ReceiverPointer(ReceiverPointer.Type.QUEUE, "Q2");
        consistentHashEx.bind(q2, "64", Map.of());
        ReceiverPointer q3 = new ReceiverPointer(ReceiverPointer.Type.QUEUE, "Q3");
        consistentHashEx.bind(q3, " ", Map.of());
        ReceiverPointer q4 = new ReceiverPointer(ReceiverPointer.Type.QUEUE, "Q4");
        consistentHashEx.bind(q4, "AA", Map.of());
        consistentHashEx.unbind(q4, "AA", Map.of());

        int messagesCount = 1_000_000;
        Map<ReceiverPointer, Long> deliveriesByReceiver = IntStream.range(0, messagesCount)
            .mapToObj(i -> consistentHashEx.selectReceiver(UUID.randomUUID().toString(), null))
            .map(Optional::get)
            .collect(Collectors.groupingBy(Function.identity(), Collectors.counting()));

        assertThat(deliveriesByReceiver).containsOnlyKeys(q1, q2, q3);

        assertThat(Long.valueOf(deliveriesByReceiver.get(q1)).doubleValue() / messagesCount).isCloseTo(0.25D, within(0.01));
        assertThat(Long.valueOf(deliveriesByReceiver.get(q2)).doubleValue() / messagesCount).isCloseTo(0.5D, within(0.01));
        assertThat(Long.valueOf(deliveriesByReceiver.get(q3)).doubleValue() / messagesCount).isCloseTo(0.25D, within(0.01));
    }
}
