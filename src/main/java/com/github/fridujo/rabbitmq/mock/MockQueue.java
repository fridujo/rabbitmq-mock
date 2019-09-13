package com.github.fridujo.rabbitmq.mock;

import static com.github.fridujo.rabbitmq.mock.tool.Exceptions.runAndEatExceptions;
import static com.github.fridujo.rabbitmq.mock.tool.Exceptions.runAndTransformExceptions;

import java.io.IOException;
import java.time.Instant;
import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.LinkedHashSet;
import java.util.Map;
import java.util.Optional;
import java.util.Queue;
import java.util.Set;
import java.util.concurrent.Executors;
import java.util.concurrent.PriorityBlockingQueue;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.function.Supplier;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.github.fridujo.rabbitmq.mock.tool.NamedThreadFactory;
import com.github.fridujo.rabbitmq.mock.tool.RestartableExecutorService;
import com.rabbitmq.client.AMQP;
import com.rabbitmq.client.AMQP.BasicProperties;
import com.rabbitmq.client.Consumer;
import com.rabbitmq.client.Envelope;
import com.rabbitmq.client.GetResponse;

public class MockQueue implements Receiver {
    private static final Logger LOGGER = LoggerFactory.getLogger(MockQueue.class);
    private static final long SLEEPING_TIME_BETWEEN_SUBMISSIONS_TO_CONSUMERS = 30L;

    private final String name;
    private final ReceiverPointer pointer;
    private final AmqArguments arguments;
    private final ReceiverRegistry receiverRegistry;
    private final MockChannel mockChannel;
    private final Queue<Message> messages;
    private final RestartableExecutorService executorService;
    private final Map<String, ConsumerAndTag> consumersByTag = new LinkedHashMap<>();
    private final AtomicInteger consumerRollingSequence = new AtomicInteger();
    private final AtomicInteger messageSequence = new AtomicInteger();
    private final Map<Long, Message> unackedMessagesByDeliveryTag = new LinkedHashMap<>();
    private final AtomicBoolean running = new AtomicBoolean(true);

    public MockQueue(String name, AmqArguments arguments, ReceiverRegistry receiverRegistry, MockChannel mockChannel) {
        this.name = name;
        this.pointer = new ReceiverPointer(ReceiverPointer.Type.QUEUE, name);
        this.arguments = arguments;
        this.receiverRegistry = receiverRegistry;
        this.mockChannel = mockChannel;

        messages = new PriorityBlockingQueue<>(11, new MessageComparator(arguments));
        executorService = new RestartableExecutorService(() -> Executors.newFixedThreadPool(1, new NamedThreadFactory(i -> name + "_queue_consuming")));
        start();
    }

    private void start() {
        executorService.submit(() -> {
            while (running.get()) {
                while (deliverToConsumerIfPossible()) ;
                runAndTransformExceptions(
                    () -> TimeUnit.MILLISECONDS.sleep(SLEEPING_TIME_BETWEEN_SUBMISSIONS_TO_CONSUMERS),
                    e -> new RuntimeException("Queue " + name + " consumer Thread have been interrupted", e));
            }
        });
    }

    private boolean deliverToConsumerIfPossible() {
        // break the delivery loop in case of a shutdown
        if (!running.get()) {
            LOGGER.debug(localized("shutting down"));
            return false;
        }
        boolean delivered = false;


        delivered = deadLetterFirstMessageIfExpired();

        if (consumersByTag.size() > 0) {
            Message message = messages.poll();
            if (message != null) {
                if (message.isExpired()) {
                    deadLetterWithReason(message, DeadLettering.ReasonType.EXPIRED);
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
                        LOGGER.debug(localized("delivering message to consumer"));
                        nextConsumer.consumer.handleDelivery(nextConsumer.tag, envelope, message.props, message.body);
                        mockChannel.getMetricsCollector().consumedMessage(mockChannel, deliveryTag, nextConsumer.tag);
                        if (nextConsumer.autoAck) {
                            unackedMessagesByDeliveryTag.remove(deliveryTag);
                        }
                        delivered = true;
                    } catch (IOException e) {
                        LOGGER.warn(localized("Unable to deliver message to consumer [" + nextConsumer.tag + "]"));
                        basicReject(deliveryTag, true);
                    }
                }
            } else {
                LOGGER.trace(localized("no consumer to deliver message to"));
            }
        }
        return delivered;
    }

    private boolean deadLetterFirstMessageIfExpired() {
        Message potentiallyExpired = messages.peek();
        if (potentiallyExpired != null && potentiallyExpired.isExpired()) {
            Message expiredMessage = messages.poll();
            if (potentiallyExpired == expiredMessage) {
                deadLetterWithReason(expiredMessage, DeadLettering.ReasonType.EXPIRED);
                return true;
            }
        }
        return false;
    }

    public void publish(String exchangeName, String routingKey, AMQP.BasicProperties props, byte[] body) {
        boolean queueLengthLimitReached = queueLengthLimitReached() || queueLengthBytesLimitReached();
        if (queueLengthLimitReached && arguments.overflow() == AmqArguments.Overflow.REJECT_PUBLISH) {
            return;
        }
        Message message = new Message(
            messageSequence.incrementAndGet(),
            exchangeName,
            routingKey,
            props,
            body,
            computeExpiryTime(props)
        );
        if (message.expiryTime != -1) {
            LOGGER.debug(localized("Message published expiring at " + Instant.ofEpochMilli(message.expiryTime)) + ": " + message);
        } else {
            LOGGER.debug(localized("Message published" + ": " + message));
        }
        messages.offer(message);
        if (queueLengthLimitReached) {
            deadLetterWithReason(messages.poll(), DeadLettering.ReasonType.MAX_LEN);
        }
    }

    @Override
    public ReceiverPointer pointer() {
        return pointer;
    }

    public void basicConsume(String consumerTag, Consumer consumer, boolean autoAck, Supplier<Long> deliveryTagSupplier) {
        LOGGER.debug(localized("registering consumer"));
        consumersByTag.put(consumerTag, new ConsumerAndTag(consumerTag, consumer, autoAck, deliveryTagSupplier));
        consumer.handleConsumeOk(consumerTag);
    }

    public GetResponse basicGet(boolean autoAck, Supplier<Long> deliveryTagSupplier) {
        long deliveryTag = deliveryTagSupplier.get();
        Message message = messages.poll();
        if (message != null) {
            if (message.isExpired()) {
                deadLetterWithReason(message, DeadLettering.ReasonType.EXPIRED);
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
                LOGGER.debug(localized("basic_get a message"));
                return new GetResponse(
                    envelope,
                    message.props,
                    message.body,
                    messages.size());
            }
        } else {
            LOGGER.debug(localized("basic_get no message available"));
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
                deadLetterWithReason(nacked, DeadLettering.ReasonType.REJECTED);
            }
        }
    }

    private String localized(String message) {
        return "[Q " + name + "] " + message;
    }

    public void basicCancel(String consumerTag) {
        if (consumersByTag.containsKey(consumerTag)) {
            Consumer consumer = consumersByTag.remove(consumerTag).consumer;
            consumer.handleCancelOk(consumerTag);
        }
    }

    public synchronized void restartDeliveryLoop() {
        if(!running.get()) {
            running.set(true);
            executorService.restart();
            start();
        }
    }

    public void notifyDeleted() {
        close();
    }

    public void close() {
        running.set(false);
        cancelConsumers();
        stopDeliveryLoop();
    }

    private void stopDeliveryLoop() {
        executorService.shutdown();
        runAndEatExceptions(() ->
            executorService.awaitTermination(
                SLEEPING_TIME_BETWEEN_SUBMISSIONS_TO_CONSUMERS * 3,
                TimeUnit.MILLISECONDS)
        );
    }

    private void cancelConsumers() {
        for (ConsumerAndTag consumerAndTag : consumersByTag.values()) {
            try {
                consumerAndTag.consumer.handleCancel(consumerAndTag.tag);
            } catch (IOException e) {
                LOGGER.warn("Consumer threw an exception when notified about cancellation", e);
            }
        }
        consumersByTag.clear();
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
        long messageExpiryTimeOfQueue = arguments
            .getMessageTtlOfQueue()
            .map(this::computeExpiry)
            .orElse(-1L);
        return getMessageExpiryTime(props).orElse(messageExpiryTimeOfQueue);
    }

    private Optional<Long> getMessageExpiryTime(AMQP.BasicProperties props) {
        return Optional.ofNullable(props.getExpiration())
            .flatMap(this::toLong)
            .map(this::computeExpiry);
    }

    private Long computeExpiry(long ttl) {
        return System.currentTimeMillis() + ttl;
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
        return "Queue " + name;
    }

    private void deadLetterWithReason(Message message, DeadLettering.ReasonType reason) {
        arguments.getDeadLetterExchange()
            .flatMap(receiverRegistry::getReceiver)
            .ifPresent(deadLetterExchange -> {
                    LOGGER.debug(localized("dead-lettered to " + deadLetterExchange + ": " + message));
                    DeadLettering.Event event = new DeadLettering.Event(name, reason, message, 1);
                    BasicProperties props = event.prependOn(message.props);
                    deadLetterExchange.publish(
                        message.exchangeName,
                        arguments.getDeadLetterRoutingKey().orElse(message.routingKey),
                        props,
                        message.body);
                }
            );
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
