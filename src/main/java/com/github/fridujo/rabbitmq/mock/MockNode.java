package com.github.fridujo.rabbitmq.mock;

import java.util.Map;
import java.util.Optional;
import java.util.concurrent.ConcurrentHashMap;
import java.util.function.Supplier;

import com.rabbitmq.client.AMQP;
import com.rabbitmq.client.Consumer;
import com.rabbitmq.client.GetResponse;
import com.rabbitmq.client.impl.AMQImpl;

import com.github.fridujo.rabbitmq.mock.configuration.Configuration;
import com.github.fridujo.rabbitmq.mock.exchange.MockDefaultExchange;
import com.github.fridujo.rabbitmq.mock.exchange.MockExchange;
import com.github.fridujo.rabbitmq.mock.exchange.MockExchangeFactory;

public class MockNode implements ReceiverRegistry, TransactionalOperations {

    private final Configuration configuration = new Configuration();
    private final MockExchangeFactory mockExchangeFactory = new MockExchangeFactory(configuration);
    private final MockDefaultExchange defaultExchange = new MockDefaultExchange(this);
    private final Map<String, MockExchange> exchanges = new ConcurrentHashMap<>();
    private final Map<String, MockQueue> queues = new ConcurrentHashMap<>();
    private final RandomStringGenerator consumerTagGenerator = new RandomStringGenerator(
        "amq.ctag-",
        "1234567890ABCDEFGHIJKLMNOPQRSTUVWXYZabcdefghijklmnopqrstuvwxyz",
        22);


    public MockNode() {
        exchanges.put(MockDefaultExchange.NAME, defaultExchange);
    }

    public boolean basicPublish(String exchangeName, String routingKey, boolean mandatory, boolean immediate, AMQP.BasicProperties props, byte[] body) {
        MockExchange exchange = getExchangeUnchecked(exchangeName);
        return exchange.publish(null, routingKey, props, body);
    }

    public String basicConsume(String queueName, boolean autoAck, String consumerTag, boolean noLocal, boolean exclusive, Map<String, Object> arguments, Consumer callback, Supplier<Long> deliveryTagSupplier, MockConnection mockConnection) {
        final String definitiveConsumerTag;
        if ("".equals(consumerTag)) {
            definitiveConsumerTag = consumerTagGenerator.generate();
        } else {
            definitiveConsumerTag = consumerTag;
        }

        getQueueUnchecked(queueName).basicConsume(definitiveConsumerTag, callback, autoAck, deliveryTagSupplier, mockConnection);

        return definitiveConsumerTag;
    }

    public Optional<MockQueue> getQueue(String name) {
        return Optional.ofNullable(queues.get(name));
    }

    public AMQP.Exchange.DeclareOk exchangeDeclare(String exchangeName, String type, boolean durable, boolean autoDelete, boolean internal, Map<String, Object> arguments) {
        exchanges.putIfAbsent(exchangeName, mockExchangeFactory.build(exchangeName, type, new AmqArguments(arguments), this));
        return new AMQImpl.Exchange.DeclareOk();
    }

    public AMQP.Exchange.DeleteOk exchangeDelete(String exchange) {
        exchanges.remove(exchange);
        return new AMQImpl.Exchange.DeleteOk();
    }

    public AMQP.Exchange.BindOk exchangeBind(String destinationName, String sourceName, String routingKey, Map<String, Object> arguments) {
        MockExchange source = getExchangeUnchecked(sourceName);
        MockExchange destination = getExchangeUnchecked(destinationName);
        source.bind(destination.pointer(), routingKey, arguments);
        return new AMQImpl.Exchange.BindOk();
    }

    public AMQP.Exchange.UnbindOk exchangeUnbind(String destinationName, String sourceName, String routingKey, Map<String, Object> arguments) {
        MockExchange source = getExchangeUnchecked(sourceName);
        MockExchange destination = getExchangeUnchecked(destinationName);
        source.unbind(destination.pointer(), routingKey);
        return new AMQImpl.Exchange.UnbindOk();
    }

    public AMQP.Queue.DeclareOk queueDeclare(String queueName, boolean durable, boolean exclusive, boolean autoDelete, Map<String, Object> arguments, MockChannel mockChannel) {
        queues.putIfAbsent(queueName, new MockQueue(queueName, new AmqArguments(arguments), this, mockChannel));
        return new AMQP.Queue.DeclareOk.Builder()
            .queue(queueName)
            .build();
    }

    public AMQP.Queue.DeleteOk queueDelete(String queueName, boolean ifUnused, boolean ifEmpty) {
        Optional<MockQueue> queue = Optional.ofNullable(queues.remove(queueName));
        queue.ifPresent(MockQueue::notifyDeleted);
        return new AMQImpl.Queue.DeleteOk(queue.map(MockQueue::messageCount).orElse(0));
    }

    public AMQP.Queue.BindOk queueBind(String queueName, String exchangeName, String routingKey, Map<String, Object> arguments) {
        MockExchange exchange = getExchangeUnchecked(exchangeName);
        MockQueue queue = getQueueUnchecked(queueName);
        exchange.bind(queue.pointer(), routingKey, arguments);
        return new AMQImpl.Queue.BindOk();
    }

    public AMQP.Queue.UnbindOk queueUnbind(String queueName, String exchangeName, String routingKey, Map<String, Object> arguments) {
        MockExchange exchange = getExchangeUnchecked(exchangeName);
        MockQueue queue = getQueueUnchecked(queueName);
        exchange.unbind(queue.pointer(), routingKey);
        return new AMQImpl.Queue.UnbindOk();
    }

    public AMQP.Queue.PurgeOk queuePurge(String queueName) {
        MockQueue queue = getQueueUnchecked(queueName);
        int messageCount = queue.purge();
        return new AMQImpl.Queue.PurgeOk(messageCount);
    }

    public GetResponse basicGet(String queueName, boolean autoAck, Supplier<Long> deliveryTagSupplier) {
        MockQueue queue = getQueueUnchecked(queueName);
        return queue.basicGet(autoAck, deliveryTagSupplier);
    }

    public void basicAck(long deliveryTag, boolean multiple) {
        queues.values().forEach(q -> q.basicAck(deliveryTag, multiple));
    }

    public void basicNack(long deliveryTag, boolean multiple, boolean requeue) {
        queues.values().forEach(q -> q.basicNack(deliveryTag, multiple, requeue));
    }

    public void basicReject(long deliveryTag, boolean requeue) {
        queues.values().forEach(q -> q.basicReject(deliveryTag, requeue));
    }

    public void basicCancel(String consumerTag) {
        queues.values().forEach(q -> q.basicCancel(consumerTag));
    }

    public AMQP.Basic.RecoverOk basicRecover(boolean requeue) {
        queues.values().forEach(q -> q.basicRecover(requeue));
        return new AMQImpl.Basic.RecoverOk();
    }

    @Override
    public Optional<Receiver> getReceiver(ReceiverPointer receiverPointer) {
        final Optional<Receiver> receiver;
        if (receiverPointer.type == ReceiverPointer.Type.EXCHANGE) {
            receiver = Optional.ofNullable(exchanges.get(receiverPointer.name));
        } else {
            receiver = Optional.ofNullable(queues.get(receiverPointer.name));
        }
        return receiver;
    }


    private MockExchange getExchangeUnchecked(String exchangeName) {
        if (!exchanges.containsKey(exchangeName)) {
            throw new IllegalArgumentException("No exchange named " + exchangeName);
        }
        return exchanges.get(exchangeName);
    }

    private MockQueue getQueueUnchecked(String queueName) {
        if (!queues.containsKey(queueName)) {
            throw new IllegalArgumentException("No queue named " + queueName);
        }
        return queues.get(queueName);
    }

    Optional<MockExchange> getExchange(String name) {
        return Optional.ofNullable(exchanges.get(name));
    }

    public int messageCount(String queueName) {
        MockQueue queue = getQueueUnchecked(queueName);
        return queue.messageCount();
    }

    public long consumerCount(String queueName) {
        MockQueue queue = getQueueUnchecked(queueName);
        return queue.consumerCount();
    }

    public MockNode restartDeliveryLoops() {
        queues.values().forEach(MockQueue::restartDeliveryLoop);
        return this;
    }

    public void close(MockConnection mockConnection) {
        queues.values().forEach(q -> q.close(mockConnection));
    }

    public Configuration getConfiguration() {
        return configuration;
    }
}
