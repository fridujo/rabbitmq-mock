package com.github.fridujo.rabbitmq.mock;

import com.rabbitmq.client.AMQP;
import com.rabbitmq.client.Consumer;
import com.rabbitmq.client.Envelope;
import com.rabbitmq.client.GetResponse;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.LinkedHashSet;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.Queue;
import java.util.Set;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;

public class MockQueue implements Receiver {
    private static final Logger LOGGER = LoggerFactory.getLogger(MockQueue.class);

    private final String name;
    private final ReceiverPointer pointer;
    private final List<ConsumerAndTag> consumers = new ArrayList<>();
    private final AtomicInteger sequence = new AtomicInteger();
    private final Queue<Message> messages = new LinkedList<>();
    private final Map<Long, Message> unackedMessagesByDeliveryTag = new LinkedHashMap<>();
    private final ExecutorService executorService = Executors.newFixedThreadPool(1);

    public MockQueue(String name) {
        this.name = name;
        this.pointer = new ReceiverPointer(ReceiverPointer.Type.QUEUE, name);
        start();
    }

    private void start() {
        executorService.submit(() -> {
            while (true) {
                while (deliverToConsumerIfPossible()) ;
                TimeUnit.MILLISECONDS.sleep(30L);
            }
        });
    }

    private boolean deliverToConsumerIfPossible() {
        boolean delivered = false;
        if (consumers.size() > 0) {
            long deliveryTag = createDeliveryTag();
            Message message = messages.poll();
            if (message != null) {
                unackedMessagesByDeliveryTag.put(deliveryTag, message);

                int index = sequence.incrementAndGet() % consumers.size();
                ConsumerAndTag nextConsumer = consumers.get(index);
                Envelope envelope = new Envelope(deliveryTag,
                    false,
                    message.exchangeName,
                    message.routingKey);
                try {
                    nextConsumer.consumer.handleDelivery(nextConsumer.tag, envelope, message.props, message.body);
                    if (nextConsumer.autoAck) {
                        unackedMessagesByDeliveryTag.remove(deliveryTag);
                    }
                    delivered = true;
                } catch (IOException e) {
                    LOGGER.warn("Unable to deliver message to consumer [" + nextConsumer.tag + "]");
                    messages.offer(unackedMessagesByDeliveryTag.remove(deliveryTag));
                }
            }
        }
        return delivered;
    }

    private long createDeliveryTag() {
        return System.currentTimeMillis() + name.hashCode();
    }

    public void publish(String exchangeName, String routingKey, AMQP.BasicProperties props, byte[] body) {
        messages.offer(new Message(
            exchangeName,
            routingKey,
            props,
            body
        ));
    }

    @Override
    public ReceiverPointer pointer() {
        return pointer;
    }

    public void addConsumer(String consumerTag, Consumer consumer, boolean autoAck) {
        consumers.add(new ConsumerAndTag(consumerTag, consumer, autoAck));
    }

    public GetResponse basicGet(boolean autoAck) {
        long deliveryTag = createDeliveryTag();
        Message message = messages.poll();
        if (message != null) {
            if (!autoAck) {
                unackedMessagesByDeliveryTag.put(deliveryTag, message);
            }
            Envelope envelope = new Envelope(
                deliveryTag,
                false,
                message.exchangeName,
                message.routingKey);
            return new GetResponse(
                envelope,
                message.props,
                message.body,
                messages.size());
        } else {
            return null;
        }
    }

    public void basicAck(long deliveryTag, boolean multiple) {
        if (multiple) {
            doWithUnackedUntil(deliveryTag, unackedMessagesByDeliveryTag::remove);
        } else {
            unackedMessagesByDeliveryTag.remove(deliveryTag);
        }
    }

    public void basicNack(long deliveryTag, boolean multiple, boolean requeue) {
        if (multiple) {
            doWithUnackedUntil(deliveryTag, relevantDeliveryTag -> basicReject(relevantDeliveryTag, requeue));
        } else {
            basicReject(deliveryTag, requeue);
        }
    }

    public void basicReject(long deliveryTag, boolean requeue) {
        Message nacked = unackedMessagesByDeliveryTag.remove(deliveryTag);
        if (requeue) {
            messages.offer(nacked);
        }
    }

    public int messageCount() {
        return messages.size();
    }

    public int consumerCount() {
        return consumers.size();
    }

    public int purge() {
        int messageCount = messageCount();
        messages.clear();
        return messageCount;
    }

    private void doWithUnackedUntil(long maxDeliveryTag, java.util.function.Consumer<Long> doWithRelevantDeliveryTag) {
        if (unackedMessagesByDeliveryTag.containsKey(maxDeliveryTag)) {
            Set<Long> storedDeliveryTagsToRemove = new LinkedHashSet<>();
            for (Long storedDeliveryTag : unackedMessagesByDeliveryTag.keySet()) {
                storedDeliveryTagsToRemove.add(storedDeliveryTag);
                if (Long.valueOf(maxDeliveryTag).equals(storedDeliveryTag)) {
                    break;
                }
            }
            storedDeliveryTagsToRemove.forEach(doWithRelevantDeliveryTag);
        }
    }

    static class ConsumerAndTag {
        private final String tag;
        private final Consumer consumer;
        private final boolean autoAck;

        ConsumerAndTag(String tag, Consumer consumer, boolean autoAck) {
            this.tag = tag;
            this.consumer = consumer;
            this.autoAck = autoAck;
        }
    }

    static class Message {
        final String exchangeName;
        final String routingKey;
        final AMQP.BasicProperties props;
        final byte[] body;

        Message(String exchangeName, String routingKey, AMQP.BasicProperties props, byte[] body) {
            this.exchangeName = exchangeName;
            this.routingKey = routingKey;
            this.props = props;
            this.body = body;
        }
    }
}
