package com.github.fridujo.rabbitmq.mock.exchange;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import com.github.fridujo.rabbitmq.mock.AmqArguments;
import com.github.fridujo.rabbitmq.mock.ReceiverPointer;
import com.github.fridujo.rabbitmq.mock.ReceiverRegistry;
import com.rabbitmq.client.AMQP;

/**
 * Mimic the behavior of <b>rabbitmq_consistent_hash_exchange</b>.
 * <p>
 * See <a href="https://github.com/rabbitmq/rabbitmq-consistent-hash-exchange">https://github.com/rabbitmq/rabbitmq-consistent-hash-exchange</a>.
 */
public class ConsistentHashExchange extends SingleReceiverExchange {

    public static final String TYPE = "x-consistent-hash";
    private final List<Bucket> buckets = new ArrayList<>();

    public ConsistentHashExchange(String name, AmqArguments arguments, ReceiverRegistry receiverRegistry) {
        super(name, TYPE, arguments, receiverRegistry);
    }

    @Override
    protected Optional<ReceiverPointer> selectReceiver(String routingKey, AMQP.BasicProperties props) {
        int bucketSelector = Math.abs(routingKey.hashCode()) % buckets.size();
        return Optional.of(buckets.get(bucketSelector).receiverPointer);
    }

    @Override
    public void bind(ReceiverPointer receiver, String routingKey, Map<String, Object> arguments) {
        super.bind(receiver, routingKey, arguments);
        buckets.addAll(bucketsFor(routingKey, receiver));
    }

    @Override
    public void unbind(ReceiverPointer receiver, String routingKey) {
        super.unbind(receiver, routingKey);
        buckets.removeIf(b -> b.receiverPointer.equals(receiver));
    }

    /**
     * When a queue is bound to a Consistent Hash exchange,
     * the binding key is a number-as-a-string which indicates the binding weight:
     * the number of buckets (sections of the range) that will be associated with the target queue.
     * <p>
     * The routing key is supposed to be an integer, {@code Object#hashCode} is used otherwise.
     */
    private int routingKeyToWeight(String routingKey) {
        try {
            return Integer.parseInt(routingKey);
        } catch (NumberFormatException e) {
            throw new IllegalArgumentException("The binding key must be an integer");
        }
    }

    private List<Bucket> bucketsFor(String routingKey, ReceiverPointer receiverPointer) {
        int weight = routingKeyToWeight(routingKey);
        return Stream.generate(() -> receiverPointer)
            .map(Bucket::new)
            .limit(weight)
            .collect(Collectors.toList());
    }

    public static final class Bucket {
        private final ReceiverPointer receiverPointer;

        public Bucket(ReceiverPointer receiverPointer) {
            this.receiverPointer = receiverPointer;
        }
    }
}
