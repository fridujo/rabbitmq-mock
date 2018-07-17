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
import java.util.Map;
import java.util.Optional;
import java.util.PriorityQueue;
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
    private final AmqArguments arguments;
    private final ReceiverRegistry receiverRegistry;
    private final MockChannel mockChannel;
    private final Queue<Message> messages;
    private final Map<String, ConsumerAndTag> consumersByTag = new LinkedHashMap<>();
    private final AtomicInteger consumerRollingSequence = new AtomicInteger();
    private final AtomicInteger messageSequence = new AtomicInteger();
    private final Map<Long, Message> unackedMessagesByDeliveryTag = new LinkedHashMap<>();
    private final ExecutorService executorService = Executors.newFixedThreadPool(1);

    public MockQueue(String name, AmqArguments arguments, ReceiverRegistry receiverRegistry, MockChannel mockChannel) {
        this.name = name;
        this.pointer = new ReceiverPointer(ReceiverPointer.Type.QUEUE, name);
        this.arguments = arguments;
        this.receiverRegistry = receiverRegistry;
        this.mockChannel = mockChannel;

        messages = new PriorityQueue<>(new MessageComparator(arguments));
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
                if (message.isExpired()) {
                    deadLetter(message);
                } else {
                    int index = consumerRollingSequence.incrementAndGet() % consumersByTag.size();
                    ConsumerAndTag nextConsumer = new ArrayList<>(consumersByTag.values()).get(index);
                    long deliveryTag = nextConsumer.deliveryTagSupplier.get();
                    unackedMessagesByDeliveryTag.put(deliveryTag, message);

                    Envelope envelope = new Envelope(deliveryTag,
                        false,
                        message.exchangeName,
                        message.routingKey);
                    try {
                        nextConsumer.consumer.handleDelivery(nextConsumer.tag, envelope, message.props, message.body);
                        mockChannel.getMetricsCollector().consumedMessage(mockChannel, deliveryTag, nextConsumer.tag);
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
        }
        return delivered;
    }

    public void publish(String exchangeName, String routingKey, AMQP.BasicProperties props, byte[] body) {
        boolean queueLengthLimitReached = queueLengthLimitReached() || queueLengthBytesLimitReached();
        if (queueLengthLimitReached && arguments.overflow() == AmqArguments.Overflow.REJECT_PUBLISH) {
            return;
        }
        messages.offer(new Message(
            messageSequence.incrementAndGet(),
            exchangeName,
            routingKey,
            props,
            body,
            computeExpiryTime(props)
        ));
        if (queueLengthLimitReached) {
            deadLetter(messages.poll());
        }
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
            if (message.isExpired()) {
                deadLetter(message);
                return null;
            } else {
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
            }
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
        if (nacked != null) {
            if (requeue) {
                messages.offer(nacked);
            } else {
                deadLetter(nacked);
            }
        }
    }

    private void deadLetter(Message message) {
        arguments.getDeadLetterExchange()
            .flatMap(receiverRegistry::getReceiver)
            .ifPresent(deadLetterExchange -> deadLetterExchange.publish(
                message.exchangeName,
                message.routingKey,
                message.props,
                message.body)
            );
    }

    public void basicCancel(String consumerTag) {
        if (consumersByTag.containsKey(consumerTag)) {
            Consumer consumer = consumersByTag.remove(consumerTag).consumer;
            consumer.handleCancelOk(consumerTag);
        }
    }

    public void notifyDeleted() {
        for (ConsumerAndTag consumerAndTag : consumersByTag.values()) {
            try {
                consumerAndTag.consumer.handleCancel(consumerAndTag.tag);
            } catch (IOException e) {
                LOGGER.warn("Consumer threw an exception when notified about cancellation", e);
            }
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

    private boolean queueLengthLimitReached() {
        return arguments.queueLengthLimit()
            .map(limit -> limit <= messages.size())
            .orElse(false);
    }

    private boolean queueLengthBytesLimitReached() {
        int messageBytesReady = messages.stream().mapToInt(m -> m.body.length).sum();
        return arguments.queueLengthBytesLimit()
            .map(limit -> limit <= messageBytesReady)
            .orElse(false);
    }

    private long computeExpiryTime(AMQP.BasicProperties props) {
        Optional<Long> messageTtlOfQueue = arguments.getMessageTtlOfQueue();
        return getMessageTtl(props)
            .orElseGet(() ->
                messageTtlOfQueue
                    .map(ttl -> System.currentTimeMillis() + ttl)
                    .orElse(-1L)
            );
    }

    private Optional<Long> getMessageTtl(AMQP.BasicProperties props) {
        return Optional.ofNullable(props.getExpiration())
            .flatMap(this::toLong);
    }

    private Optional<Long> toLong(String s) {
        try {
            return Optional.of(Long.parseLong(s));
        } catch (NumberFormatException e) {
            return Optional.empty();
        }
    }

    @Override
    public String toString() {
        return "MockQueue{" +
            "name='" + name + '\'' +
            ", arguments=" + arguments +
            '}';
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
}
