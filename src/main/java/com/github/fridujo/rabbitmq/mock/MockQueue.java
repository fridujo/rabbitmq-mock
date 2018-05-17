package com.github.fridujo.rabbitmq.mock;

import com.rabbitmq.client.AMQP;
import com.rabbitmq.client.Consumer;
import com.rabbitmq.client.Envelope;
import com.rabbitmq.client.GetResponse;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.io.UncheckedIOException;
import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.LinkedHashSet;
import java.util.LinkedList;
import java.util.Map;
import java.util.Queue;
import java.util.Set;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.function.Supplier;

public class MockQueue implements Receiver {
    private static final Logger LOGGER = LoggerFactory.getLogger(MockQueue.class);

    private final String name;
    private final ReceiverPointer pointer;
    private final Map<String, ConsumerAndTag> consumersByTag = new LinkedHashMap<>();
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
        if (consumersByTag.size() > 0) {
            Message message = messages.poll();
            if (message != null) {
                int index = sequence.incrementAndGet() % consumersByTag.size();
                ConsumerAndTag nextConsumer = new ArrayList<>(consumersByTag.values()).get(index);
                long deliveryTag = nextConsumer.deliveryTagSupplier.get();
                unackedMessagesByDeliveryTag.put(deliveryTag, message);

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
                    basicReject(deliveryTag, true);
                }
            }
        }
        return delivered;
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

    public void basicConsume(String consumerTag, Consumer consumer, boolean autoAck, Supplier<Long> deliveryTagSupplier) {
        consumersByTag.put(consumerTag, new ConsumerAndTag(consumerTag, consumer, autoAck, deliveryTagSupplier));
        consumer.handleConsumeOk(consumerTag);
    }

    public GetResponse basicGet(boolean autoAck, Supplier<Long> deliveryTagSupplier) {
        long deliveryTag = deliveryTagSupplier.get();
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

    public void basicCancel(String consumerTag) {
        if (consumersByTag.containsKey(consumerTag)) {
            Consumer consumer = consumersByTag.remove(consumerTag).consumer;
            try {
                consumer.handleCancel(consumerTag);
            } catch (IOException e) {
                throw new UncheckedIOException(e);
            }
            consumer.handleCancelOk(consumerTag);
        }
    }

    public void basicRecover(boolean requeue) {
        Set<Long> unackedDeliveryTags = new LinkedHashSet<>(unackedMessagesByDeliveryTag.keySet());
        unackedDeliveryTags.forEach(unackedDeliveryTag -> messages.offer(unackedMessagesByDeliveryTag.remove(unackedDeliveryTag)));
        consumersByTag.values().forEach(consumerAndTag -> consumerAndTag.consumer.handleRecoverOk(consumerAndTag.tag));
    }

    public int messageCount() {
        return messages.size();
    }

    public int consumerCount() {
        return consumersByTag.size();
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
        private final Supplier<Long> deliveryTagSupplier;

        ConsumerAndTag(String tag, Consumer consumer, boolean autoAck, Supplier<Long> deliveryTagSupplier) {
            this.tag = tag;
            this.consumer = consumer;
            this.autoAck = autoAck;
            this.deliveryTagSupplier = deliveryTagSupplier;
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
