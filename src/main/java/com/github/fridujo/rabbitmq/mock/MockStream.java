package com.github.fridujo.rabbitmq.mock;

import com.github.fridujo.rabbitmq.mock.tool.NamedThreadFactory;
import com.github.fridujo.rabbitmq.mock.tool.RestartableExecutorService;
import com.rabbitmq.client.AMQP.BasicProperties;
import com.rabbitmq.client.Consumer;
import com.rabbitmq.client.Envelope;
import com.rabbitmq.client.GetResponse;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.time.Instant;
import java.util.ArrayList;
import java.util.Date;
import java.util.HashMap;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.function.Supplier;

import static com.github.fridujo.rabbitmq.mock.tool.Exceptions.runAndEatExceptions;
import static com.github.fridujo.rabbitmq.mock.tool.Exceptions.runAndTransformExceptions;

public class MockStream extends MockQueue implements Receiver {
    private static final Logger LOGGER = LoggerFactory.getLogger(MockStream.class);
    private static final long SLEEPING_TIME_BETWEEN_SUBMISSIONS_TO_CONSUMERS = 30L;

    private final String name;
    private final ReceiverPointer pointer;
    private final AmqArguments arguments;
    private final List<Message> messageStream = new ArrayList<>();
    private final Map<Long, Message> messageByOffset = new HashMap<>();
    private final RestartableExecutorService executorService;
    private final Map<String, ConsumerAndTag> consumersByTag = new LinkedHashMap<>();
    private final AtomicInteger consumerRollingSequence = new AtomicInteger();
    private final AtomicInteger messageSequence = new AtomicInteger();
    private final AtomicBoolean running = new AtomicBoolean(true);
    private final Map<ConsumerAndTag, Long> unackedMessageByConsumer = new HashMap<>();
    private ConsumerAndTag currentConsumer;

    public MockStream(String name, AmqArguments arguments, ReceiverRegistry receiverRegistry) {
        super(name, arguments, receiverRegistry);
        this.name = name;
        this.pointer = new ReceiverPointer(ReceiverPointer.Type.QUEUE, name);
        this.arguments = arguments;
        executorService = new RestartableExecutorService(() -> Executors.newFixedThreadPool(1, new NamedThreadFactory(i -> name + "_queue_consuming")));
        start();
    }

    private void start() {
        executorService.submit(() -> {
            while (running.get()) {
                while (deliverToConsumerIfPossible()) ;
                runAndTransformExceptions(
                    () -> TimeUnit.MILLISECONDS.sleep(SLEEPING_TIME_BETWEEN_SUBMISSIONS_TO_CONSUMERS),
                    e -> new RuntimeException("Queue " + name + " consumer Thread have been interrupted", e)
                );
            }
        });
    }

    private Long calculateInitialOffset(ConsumerAndTag consumer) {

        final Map<String, Object> arguments = consumer.arguments;
        Object o = arguments.getOrDefault("x-stream-offset", "next");

        if (messageByOffset.isEmpty()) {
            return 0L;
        } else {
            Long minOffset = messageByOffset.keySet().stream().mapToLong(Long::longValue).min().orElse(0);
            Long maxOffset = messageByOffset.keySet().stream().mapToLong(Long::longValue).max().orElse(0);
            Message first = messageByOffset.get(minOffset);
            Message last = messageByOffset.get(maxOffset);
            Date firstTimestamp = first == null ? null : first.props.getTimestamp();
            Date lastTimestamp = last == null ? null : last.props.getTimestamp();
            return calculateOffset(o, minOffset, maxOffset, firstTimestamp, lastTimestamp);
        }
    }

    private Long calculateOffset(Object offsetArg, Long minOffset, Long maxOffset, Date firstTimestamp, Date lastTimestamp) {

        if (offsetArg instanceof String) {
            final String s = (String) offsetArg;
            if (s.equals("first")) {
                return calculateOffset(0, minOffset, maxOffset, firstTimestamp, lastTimestamp);
            } else if (s.equals("next")) {
                return messageByOffset.keySet().stream().mapToLong(Long::longValue).max().orElse(0);
            } else {
                String msg = String.format("Cannot understand value [%s] for x-stream-offset", s);
                throw new RuntimeException(msg);
            }
        } else if (offsetArg instanceof Long) {
            final Long l = (Long) offsetArg;
            if (l < 0) {
                return calculateOffset("next", minOffset, maxOffset, firstTimestamp, lastTimestamp);
            } else {
                if (l < minOffset) {
                    return minOffset;
                } else if (l > maxOffset) {
                    return maxOffset + 1;
                } else {
                    return l;
                }
            }
        } else if (offsetArg instanceof Date) {
            final Date d = (Date) offsetArg;
            if (d.before(firstTimestamp)) {
                return minOffset;
            } else if (d.after(lastTimestamp)) {
                return maxOffset + 1;
            } else if (d.equals(firstTimestamp)) {
                return calculateOffset(0, minOffset, maxOffset, firstTimestamp, lastTimestamp);
            } else {
                final Long o1 = messageByOffset.entrySet().stream()
                    .filter(p -> d.before(p.getValue().props.getTimestamp()))
                    .findFirst()
                    .orElseGet(() -> new Map.Entry<Long, Message>() {
                        @Override public Long getKey() { return 0L; }
                        @Override public Message getValue() { return null; }
                        @Override public Message setValue(Message value) { return null; }
                    })
                    .getKey();
                final Long o2 = o1 + 1;
                final Message m1 = messageByOffset.get(o1);
                final Message m2 = messageByOffset.get(o2);
                final long diff1 = d.getTime() - m1.props.getTimestamp().getTime();
                final long diff2 = m2.props.getTimestamp().getTime() - d.getTime();
                return (diff1 < diff2) ? o1 : o2;
            }
        }
        return 0L;
    }

    private boolean deliverToConsumerIfPossible() {
        // break the delivery loop in case of a shutdown
        if (!running.get()) {
            LOGGER.debug(localized("shutting down"));
            return false;
        }
        boolean delivered = false;

        if (!consumersByTag.isEmpty()) {
            int index = consumerRollingSequence.incrementAndGet() % consumersByTag.size();
            ConsumerAndTag nextConsumer = new ArrayList<>(consumersByTag.values()).get(index);
            currentConsumer = nextConsumer;

            if (unackedMessageByConsumer.containsKey(nextConsumer)) {
                return true;
            }

            final long streamOffset = nextConsumer.streamOffset;

            Message originalMessage = messageByOffset.get(streamOffset);
            if (originalMessage != null) {
                Message message = originalMessage.deliverWithOffset(streamOffset);
                if (!message.isExpired()) {
                    long deliveryTag = nextConsumer.deliveryTagSupplier.get();
                    unackedMessageByConsumer.put(nextConsumer, deliveryTag);
                    Envelope envelope = new Envelope(
                        deliveryTag,
                        message.redelivered,
                        message.exchangeName,
                        message.routingKey
                    );
                    try {
                        LOGGER.debug(localized("delivering message to consumer"));
                        nextConsumer.mockChannel.getMetricsCollector().consumedMessage(nextConsumer.mockChannel, deliveryTag, nextConsumer.tag);
                        // handleDelivery must be synchronous if acknowledgements are to be meaningful
                        // The entire setup is running under a single thread so no other consumers will
                        // receive a message until the current consumer is done.
                        nextConsumer.consumer.handleDelivery(nextConsumer.tag, envelope, message.props, message.body);

                        delivered = true;
                        if (unackedMessageByConsumer.getOrDefault(nextConsumer, null) == null) {
                            nextConsumer.pushOffset();
                        } else {
                            LOGGER.warn(localized("Forgot to acknowledge message. Blocking consumer [" + nextConsumer.tag + "]"));
                        }
                    } catch (IOException e) {
                        LOGGER.warn(localized("Unable to deliver message to consumer [" + nextConsumer.tag + "]"));
                    }
                } else {
                    nextConsumer.pushOffset();
                    LOGGER.trace(localized("Skipping expired message for consumer [" + nextConsumer.tag + "]"));
                }
            } else {
                LOGGER.trace(localized("no message to deliver to consumer [" + nextConsumer.tag + "]"));
            }
        } else {
            LOGGER.trace(localized("no consumer to deliver message to"));
        }
        return delivered;
    }

    public boolean publish(String exchangeName, String routingKey, BasicProperties props, byte[] body) {

        Message message = new Message(
            messageSequence.getAndIncrement(),
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
        messageStream.add(message);
        messageByOffset.put((long) message.id, message);
        return true;
    }

    @Override
    public ReceiverPointer pointer() {
        return pointer;
    }

    @Override
    public void basicConsume(String consumerTag, Consumer consumer, boolean autoAck, Supplier<Long> deliveryTagSupplier, MockConnection mockConnection, MockChannel mockChannel) {
        LOGGER.debug(localized("registering consumer"));
        Map<String, Object> arguments;
        if (consumer instanceof ConsumerWithArguments) {
            arguments = ((ConsumerWithArguments) consumer).getArguments();
        } else {
            arguments = new HashMap<String, Object>() {{ put("x-stream-offset", "next"); }};
        }
        ConsumerAndTag consumerAndTag = new ConsumerAndTag(consumerTag, consumer, autoAck, arguments, deliveryTagSupplier, mockConnection, mockChannel);
        consumerAndTag.streamOffset = calculateInitialOffset(consumerAndTag);
        consumersByTag.put(consumerTag, consumerAndTag);
        consumer.handleConsumeOk(consumerTag);
    }

    public GetResponse basicGet(boolean autoAck, Supplier<Long> deliveryTagSupplier) {

        throw new RuntimeException("Cannot get from Stream. Only consume is allowed");
    }

    public void basicAck(long deliveryTag, boolean multiple) {
        if (multiple) {
            throw new RuntimeException("You may only acknowledge Stream messages one by one");
        } else {
            unackedMessageByConsumer.remove(currentConsumer, deliveryTag);
        }
    }

    public void basicNack(long deliveryTag, boolean multiple, boolean requeue) {
        if (multiple) {
            throw new RuntimeException("You may only acknowledge Stream messages one by one");
        }
    }

    public void basicReject(long deliveryTag, boolean requeue) {
        throw new RuntimeException("Streams do not have dead lettering or requeing");
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

    synchronized void restartDeliveryLoop() {
        if (!running.get()) {
            running.set(true);
            executorService.restart();
            start();
        }
    }

    void notifyDeleted() {
        close();
    }

    void close(MockConnection mockConnection) {
        consumersByTag.entrySet().removeIf(e -> {
            final boolean mustCancelConsumer = e.getValue().mockConnection == mockConnection;
            if (mustCancelConsumer) {
                cancel(e.getValue());
            }
            return mustCancelConsumer;
        });
        if (consumersByTag.isEmpty()) {
            running.set(false);
            stopDeliveryLoop();
        }
    }

    private void close() {
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
            cancel(consumerAndTag);
        }
        consumersByTag.clear();
    }

    private void cancel(ConsumerAndTag consumerAndTag) {
        try {
            consumerAndTag.consumer.handleCancel(consumerAndTag.tag);
        } catch (IOException e) {
            LOGGER.warn("Consumer threw an exception when notified about cancellation", e);
        }
    }

    public void basicRecover(boolean requeue) {
        throw new RuntimeException("There is no recovery in Streams. You may only set the offset to go back.");
    }

    public int messageCount() {
        return messageStream.size();
    }

    public int consumerCount() {
        return consumersByTag.size();
    }

    public int purge() {
        int messageCount = messageCount();
        messageStream.clear();
        messageByOffset.clear();
        return messageCount;
    }

    private long computeExpiryTime(BasicProperties props) {
        long messageExpiryTimeOfQueue = arguments
            .getMessageTtlOfQueue()
            .map(this::computeExpiry)
            .orElse(-1L);
        return getMessageExpiryTime(props).orElse(messageExpiryTimeOfQueue);
    }

    private Optional<Long> getMessageExpiryTime(BasicProperties props) {
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

    public List<Message> getAvailableMessages() {
        return new ArrayList<>(messageStream);
    }

    public List<Message> getUnackedMessages() {
        return new ArrayList<>();
    }

    static class ConsumerAndTag {

        private final String tag;
        private final Consumer consumer;
        private final Supplier<Long> deliveryTagSupplier;
        private final MockConnection mockConnection;
        private final MockChannel mockChannel;
        private final Map<String, Object> arguments;
        private Long streamOffset;

        ConsumerAndTag(String tag, Consumer consumer, boolean autoAck, Map<String, Object> arguments, Supplier<Long> deliveryTagSupplier, MockConnection mockConnection, MockChannel mockChannel) {
            if (autoAck) {
                throw new RuntimeException("Streams do not allow autoAck");
            }
            this.tag = tag;
            this.consumer = consumer;
            this.deliveryTagSupplier = deliveryTagSupplier;
            this.mockConnection = mockConnection;
            this.mockChannel = mockChannel;
            this.arguments = arguments;
            this.streamOffset = 0L;
        }

        void pushOffset() { streamOffset++; }
    }
}
