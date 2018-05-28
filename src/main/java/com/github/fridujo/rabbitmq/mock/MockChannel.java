package com.github.fridujo.rabbitmq.mock;

import com.rabbitmq.client.AMQP;
import com.rabbitmq.client.BuiltinExchangeType;
import com.rabbitmq.client.CancelCallback;
import com.rabbitmq.client.Channel;
import com.rabbitmq.client.Command;
import com.rabbitmq.client.ConfirmCallback;
import com.rabbitmq.client.ConfirmListener;
import com.rabbitmq.client.Connection;
import com.rabbitmq.client.Consumer;
import com.rabbitmq.client.ConsumerShutdownSignalCallback;
import com.rabbitmq.client.DeliverCallback;
import com.rabbitmq.client.GetResponse;
import com.rabbitmq.client.Method;
import com.rabbitmq.client.ReturnCallback;
import com.rabbitmq.client.ReturnListener;
import com.rabbitmq.client.ShutdownListener;
import com.rabbitmq.client.ShutdownSignalException;
import com.rabbitmq.client.impl.AMQImpl;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.Collections;
import java.util.Map;
import java.util.Objects;
import java.util.Optional;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicLong;

public class MockChannel implements Channel {
    private static final Logger LOGGER = LoggerFactory.getLogger(MockChannel.class);

    private final int channelNumber;
    private final MockNode node;
    private final MockConnection mockConnection;
    private final AtomicBoolean opened = new AtomicBoolean(true);
    private final AtomicLong deliveryTagSequence = new AtomicLong();
    private final RandomStringGenerator queueNameGenerator = new RandomStringGenerator(
        "amq.gen-",
        "1234567890ABCDEFGHIJKLMNOPQRSTUVWXYZabcdefghijklmnopqrstuvwxyz",
        22);

    private String lastGeneratedQueueName;

    public MockChannel(int channelNumber, MockNode node, MockConnection mockConnection) {
        this.channelNumber = channelNumber;
        this.node = node;
        this.mockConnection = mockConnection;
    }

    @Override
    public int getChannelNumber() {
        return channelNumber;
    }

    @Override
    public Connection getConnection() {
        return mockConnection;
    }

    @Override
    public void close() {
        // Called by RabbitTemplate#execute, RabbitTemplate#send, BlockingQueueConsumer#stop
        close(AMQP.REPLY_SUCCESS, "OK");
    }

    @Override
    public void close(int closeCode, String closeMessage) {
        opened.set(false);
    }

    @Override
    public void abort() {
        abort(AMQP.REPLY_SUCCESS, "OK");
    }

    @Override
    public void abort(int closeCode, String closeMessage) {
        close(closeCode, closeMessage);
    }

    @Override
    public void addReturnListener(ReturnListener listener) {
        throw new UnsupportedOperationException();
    }

    @Override
    public ReturnListener addReturnListener(ReturnCallback returnCallback) {
        throw new UnsupportedOperationException();
    }

    @Override
    public boolean removeReturnListener(ReturnListener listener) {
        throw new UnsupportedOperationException();
    }

    @Override
    public void clearReturnListeners() {
        throw new UnsupportedOperationException();
    }

    @Override
    public void addConfirmListener(ConfirmListener listener) {
        throw new UnsupportedOperationException();
    }

    @Override
    public ConfirmListener addConfirmListener(ConfirmCallback ackCallback, ConfirmCallback nackCallback) {
        throw new UnsupportedOperationException();
    }

    @Override
    public boolean removeConfirmListener(ConfirmListener listener) {
        throw new UnsupportedOperationException();
    }

    @Override
    public void clearConfirmListeners() {
        throw new UnsupportedOperationException();
    }

    @Override
    public Consumer getDefaultConsumer() {
        throw new UnsupportedOperationException();
    }

    @Override
    public void setDefaultConsumer(Consumer consumer) {
        throw new UnsupportedOperationException();
    }

    @Override
    public void basicQos(int prefetchSize, int prefetchCount, boolean global) {
    }

    @Override
    public void basicQos(int prefetchCount, boolean global) {
        basicQos(0, prefetchCount, true);
    }

    @Override
    public void basicQos(int prefetchCount) {
        // Called when BlockingQueueConsumer#start
        basicQos(prefetchCount, true);
    }

    @Override
    public void basicPublish(String exchange, String routingKey, AMQP.BasicProperties props, byte[] body) throws IOException {
        basicPublish(exchange, routingKey, false, props, body);
    }

    @Override
    public void basicPublish(String exchange, String routingKey, boolean mandatory, AMQP.BasicProperties props, byte[] body) throws IOException {
        basicPublish(exchange, routingKey, false, false, props, body);
    }

    @Override
    public void basicPublish(String exchange, String routingKey, boolean mandatory, boolean immediate, AMQP.BasicProperties props, byte[] body) {
        node.basicPublish(exchange, routingKey, mandatory, immediate, nullToEmpty(props), body);
    }

    @Override
    public AMQP.Exchange.DeclareOk exchangeDeclare(String exchange, String type) {
        return exchangeDeclare(exchange, type, false, true, false, Collections.emptyMap());
    }

    @Override
    public AMQP.Exchange.DeclareOk exchangeDeclare(String exchange, BuiltinExchangeType type) {
        return exchangeDeclare(exchange, type, false);
    }

    @Override
    public AMQP.Exchange.DeclareOk exchangeDeclare(String exchange, String type, boolean durable) {
        return exchangeDeclare(exchange, type, durable, true, Collections.emptyMap());
    }

    @Override
    public AMQP.Exchange.DeclareOk exchangeDeclare(String exchange, BuiltinExchangeType type, boolean durable) {
        return exchangeDeclare(exchange, type, durable, true, Collections.emptyMap());
    }

    @Override
    public AMQP.Exchange.DeclareOk exchangeDeclare(String exchange, String type, boolean durable, boolean autoDelete, Map<String, Object> arguments) {
        return exchangeDeclare(exchange, type, durable, autoDelete, false, arguments);
    }

    @Override
    public AMQP.Exchange.DeclareOk exchangeDeclare(String exchange, BuiltinExchangeType type, boolean durable, boolean autoDelete, Map<String, Object> arguments) {
        return exchangeDeclare(exchange, type, durable, autoDelete, false, arguments);
    }

    @Override
    public AMQP.Exchange.DeclareOk exchangeDeclare(String exchange, String type, boolean durable, boolean autoDelete, boolean internal, Map<String, Object> arguments) {
        return node.exchangeDeclare(exchange, type, durable, autoDelete, internal, nullToEmpty(arguments));
    }

    @Override
    public AMQP.Exchange.DeclareOk exchangeDeclare(String exchange, BuiltinExchangeType type, boolean durable, boolean autoDelete, boolean internal, Map<String, Object> arguments) {
        return exchangeDeclare(exchange, type.getType(), durable, autoDelete, internal, arguments);
    }

    @Override
    public void exchangeDeclareNoWait(String exchange, String type, boolean durable, boolean autoDelete, boolean internal, Map<String, Object> arguments) {
        exchangeDeclare(exchange, type, durable, autoDelete, internal, arguments);
    }

    @Override
    public void exchangeDeclareNoWait(String exchange, BuiltinExchangeType type, boolean durable, boolean autoDelete, boolean internal, Map<String, Object> arguments) {
        exchangeDeclareNoWait(exchange, type.getType(), durable, autoDelete, internal, arguments);
    }

    @Override
    public AMQP.Exchange.DeclareOk exchangeDeclarePassive(String name) throws IOException {
        if (!node.getExchange(name).isPresent()) {
            throw new IOException(new ShutdownSignalException(
                false,
                false,
                new AMQImpl.Channel.Close(
                    404,
                    "NOT_FOUND",
                    AMQImpl.Exchange.INDEX,
                    AMQImpl.Exchange.Declare.INDEX),
                null,
                "no exchange '" + name + "' in vhost '/' ",
                null));
        }
        return new AMQImpl.Exchange.DeclareOk();
    }

    @Override
    public AMQP.Exchange.DeleteOk exchangeDelete(String exchange, boolean ifUnused) {
        return node.exchangeDelete(exchange);
    }

    @Override
    public void exchangeDeleteNoWait(String exchange, boolean ifUnused) {
        exchangeDelete(exchange, ifUnused);
    }

    @Override
    public AMQP.Exchange.DeleteOk exchangeDelete(String exchange) {
        return exchangeDelete(exchange, false);
    }

    @Override
    public AMQP.Exchange.BindOk exchangeBind(String destination, String source, String routingKey) {
        return exchangeBind(destination, source, routingKey, Collections.emptyMap());
    }

    @Override
    public AMQP.Exchange.BindOk exchangeBind(String destination, String source, String routingKey, Map<String, Object> arguments) {
        return node.exchangeBind(destination, source, routingKey, nullToEmpty(arguments));
    }

    @Override
    public void exchangeBindNoWait(String destination, String source, String routingKey, Map<String, Object> arguments) {
        exchangeBind(destination, source, routingKey, arguments);
    }

    @Override
    public AMQP.Exchange.UnbindOk exchangeUnbind(String destination, String source, String routingKey) {
        return exchangeUnbind(destination, source, routingKey, Collections.emptyMap());
    }

    @Override
    public AMQP.Exchange.UnbindOk exchangeUnbind(String destination, String source, String routingKey, Map<String, Object> arguments) {
        return node.exchangeUnbind(destination, source, routingKey, nullToEmpty(arguments));
    }

    @Override
    public void exchangeUnbindNoWait(String destination, String source, String routingKey, Map<String, Object> arguments) {
        exchangeUnbind(destination, source, routingKey, arguments);
    }

    @Override
    public AMQP.Queue.DeclareOk queueDeclare() {
        return queueDeclare("", false, true, true, Collections.emptyMap());
    }

    @Override
    public AMQP.Queue.DeclareOk queueDeclare(String queue, boolean durable, boolean exclusive, boolean autoDelete, Map<String, Object> arguments) {
        return node.queueDeclare(generateIfEmpty(queue), durable, exclusive, autoDelete, arguments);
    }

    @Override
    public void queueDeclareNoWait(String queue, boolean durable, boolean exclusive, boolean autoDelete, Map<String, Object> arguments) {
        queueDeclare(queue, durable, exclusive, autoDelete, arguments);
    }

    @Override
    public AMQP.Queue.DeclareOk queueDeclarePassive(String queueName) throws IOException {
        String definitiveQueueName = lastGeneratedIfEmpty(queueName);
        Optional<MockQueue> queue = node.getQueue(definitiveQueueName);
        if (!queue.isPresent()) {
            throw new IOException(new ShutdownSignalException(
                false,
                false,
                new AMQImpl.Channel.Close(
                    404,
                    "NOT_FOUND",
                    AMQImpl.Queue.INDEX,
                    AMQImpl.Queue.Declare.INDEX),
                null,
                "no queue '" + definitiveQueueName + "' in vhost '/' ",
                null));
        }
        return new AMQImpl.Queue.DeclareOk(definitiveQueueName, queue.get().messageCount(), queue.get().consumerCount());
    }

    @Override
    public AMQP.Queue.DeleteOk queueDelete(String queue) {
        return queueDelete(queue, false, false);
    }

    @Override
    public AMQP.Queue.DeleteOk queueDelete(String queue, boolean ifUnused, boolean ifEmpty) {
        return node.queueDelete(lastGeneratedIfEmpty(queue), ifUnused, ifEmpty);
    }

    @Override
    public void queueDeleteNoWait(String queue, boolean ifUnused, boolean ifEmpty) {
        queueDelete(queue, ifUnused, ifEmpty);
    }

    @Override
    public AMQP.Queue.BindOk queueBind(String queue, String exchange, String routingKey) {
        return queueBind(queue, exchange, routingKey, Collections.emptyMap());
    }

    @Override
    public AMQP.Queue.BindOk queueBind(String queue, String exchange, String routingKey, Map<String, Object> arguments) {
        return node.queueBind(lastGeneratedIfEmpty(queue), exchange, routingKey, nullToEmpty(arguments));
    }

    @Override
    public void queueBindNoWait(String queue, String exchange, String routingKey, Map<String, Object> arguments) {
        queueBind(queue, exchange, routingKey, arguments);
    }

    @Override
    public AMQP.Queue.UnbindOk queueUnbind(String queue, String exchange, String routingKey) {
        return queueUnbind(queue, exchange, routingKey, Collections.emptyMap());
    }

    @Override
    public AMQP.Queue.UnbindOk queueUnbind(String queue, String exchange, String routingKey, Map<String, Object> arguments) {
        return node.queueUnbind(lastGeneratedIfEmpty(queue), exchange, routingKey, nullToEmpty(arguments));
    }

    @Override
    public AMQP.Queue.PurgeOk queuePurge(String queue) {
        return node.queuePurge(lastGeneratedIfEmpty(queue));
    }

    @Override
    public GetResponse basicGet(String queue, boolean autoAck) {
        return node.basicGet(lastGeneratedIfEmpty(queue), autoAck, this::nextDeliveryTag);
    }

    @Override
    public void basicAck(long deliveryTag, boolean multiple) {
        node.basicAck(deliveryTag, multiple);
    }

    @Override
    public void basicNack(long deliveryTag, boolean multiple, boolean requeue) {
        node.basicNack(deliveryTag, multiple, requeue);
    }

    @Override
    public void basicReject(long deliveryTag, boolean requeue) {
        node.basicReject(deliveryTag, requeue);
    }

    @Override
    public String basicConsume(String queue, Consumer callback) {
        return basicConsume(queue, false, callback);
    }

    @Override
    public String basicConsume(String queue, DeliverCallback deliverCallback, CancelCallback cancelCallback) {
        return basicConsume(queue, false, deliverCallback, cancelCallback);
    }

    @Override
    public String basicConsume(String queue, DeliverCallback deliverCallback, ConsumerShutdownSignalCallback shutdownSignalCallback) {
        return basicConsume(queue, false, deliverCallback, shutdownSignalCallback);
    }

    @Override
    public String basicConsume(String queue, DeliverCallback deliverCallback, CancelCallback cancelCallback, ConsumerShutdownSignalCallback shutdownSignalCallback) {
        return basicConsume(queue, false, deliverCallback, cancelCallback, shutdownSignalCallback);
    }

    @Override
    public String basicConsume(String queue, boolean autoAck, Consumer callback) {
        return basicConsume(queue, autoAck, Collections.emptyMap(), callback);
    }

    @Override
    public String basicConsume(String queue, boolean autoAck, DeliverCallback deliverCallback, CancelCallback cancelCallback) {
        return basicConsume(queue, autoAck, Collections.emptyMap(), deliverCallback, cancelCallback);
    }

    @Override
    public String basicConsume(String queue, boolean autoAck, DeliverCallback deliverCallback, ConsumerShutdownSignalCallback shutdownSignalCallback) {
        return basicConsume(queue, autoAck, Collections.emptyMap(), deliverCallback, shutdownSignalCallback);
    }

    @Override
    public String basicConsume(String queue, boolean autoAck, DeliverCallback deliverCallback, CancelCallback cancelCallback, ConsumerShutdownSignalCallback shutdownSignalCallback) {
        return basicConsume(queue, autoAck, Collections.emptyMap(), deliverCallback, cancelCallback, shutdownSignalCallback);
    }

    @Override
    public String basicConsume(String queue, boolean autoAck, Map<String, Object> arguments, Consumer callback) {
        return basicConsume(queue, autoAck, "", false, false, arguments, callback);
    }

    @Override
    public String basicConsume(String queue, boolean autoAck, Map<String, Object> arguments, DeliverCallback deliverCallback, CancelCallback cancelCallback) {
        return basicConsume(queue, autoAck, arguments, deliverCallback, cancelCallback, null);
    }

    @Override
    public String basicConsume(String queue, boolean autoAck, Map<String, Object> arguments, DeliverCallback deliverCallback, ConsumerShutdownSignalCallback shutdownSignalCallback) {
        return basicConsume(queue, autoAck, arguments, deliverCallback, null, shutdownSignalCallback);
    }

    @Override
    public String basicConsume(String queue, boolean autoAck, Map<String, Object> arguments, DeliverCallback deliverCallback, CancelCallback cancelCallback, ConsumerShutdownSignalCallback shutdownSignalCallback) {
        return basicConsume(queue, autoAck, "", false, false, arguments, deliverCallback, cancelCallback, shutdownSignalCallback);
    }

    @Override
    public String basicConsume(String queue, boolean autoAck, String consumerTag, Consumer callback) {
        return basicConsume(queue, autoAck, consumerTag, false, false, Collections.emptyMap(), callback);
    }

    @Override
    public String basicConsume(String queue, boolean autoAck, String consumerTag, DeliverCallback deliverCallback, CancelCallback cancelCallback) {
        return basicConsume(queue, autoAck, consumerTag, deliverCallback, cancelCallback, null);
    }

    @Override
    public String basicConsume(String queue, boolean autoAck, String consumerTag, DeliverCallback deliverCallback, ConsumerShutdownSignalCallback shutdownSignalCallback) {
        return basicConsume(queue, autoAck, consumerTag, deliverCallback, null, shutdownSignalCallback);
    }

    @Override
    public String basicConsume(String queue, boolean autoAck, String consumerTag, DeliverCallback deliverCallback, CancelCallback cancelCallback, ConsumerShutdownSignalCallback shutdownSignalCallback) {
        return basicConsume(queue, autoAck, consumerTag, false, false, Collections.emptyMap(), deliverCallback, cancelCallback, shutdownSignalCallback);
    }

    @Override
    public String basicConsume(String queue, boolean autoAck, String consumerTag, boolean noLocal, boolean exclusive, Map<String, Object> arguments, Consumer callback) {
        return node.basicConsume(lastGeneratedIfEmpty(queue), autoAck, consumerTag, noLocal, exclusive, nullToEmpty(arguments), callback, this::nextDeliveryTag);
    }

    @Override
    public String basicConsume(String queue, boolean autoAck, String consumerTag, boolean noLocal, boolean exclusive, Map<String, Object> arguments, DeliverCallback deliverCallback, CancelCallback cancelCallback) {
        return basicConsume(queue, autoAck, consumerTag, noLocal, exclusive, arguments, deliverCallback, cancelCallback, null);
    }

    @Override
    public String basicConsume(String queue, boolean autoAck, String consumerTag, boolean noLocal, boolean exclusive, Map<String, Object> arguments, DeliverCallback deliverCallback, ConsumerShutdownSignalCallback shutdownSignalCallback) {
        return basicConsume(queue, autoAck, consumerTag, noLocal, exclusive, arguments, deliverCallback, null, shutdownSignalCallback);
    }

    @Override
    public String basicConsume(String queue, boolean autoAck, String consumerTag, boolean noLocal, boolean exclusive, Map<String, Object> arguments, DeliverCallback deliverCallback, CancelCallback cancelCallback, ConsumerShutdownSignalCallback shutdownSignalCallback) {
        return basicConsume(queue, autoAck, consumerTag, noLocal, exclusive, arguments, new ConsumerWrapper(deliverCallback, cancelCallback, shutdownSignalCallback));
    }

    @Override
    public void basicCancel(String consumerTag) {
        // Called when BlockingQueueConsumer#basicCancel (after AbstractMessageListenerContainer#stop)
        node.basicCancel(consumerTag);
    }

    @Override
    public AMQP.Basic.RecoverOk basicRecover() {
        return basicRecover(true);
    }

    @Override
    public AMQP.Basic.RecoverOk basicRecover(boolean requeue) {
        return node.basicRecover(true);
    }

    @Override
    public AMQP.Tx.SelectOk txSelect() {
        throw new UnsupportedOperationException();
    }

    @Override
    public AMQP.Tx.CommitOk txCommit() {
        throw new UnsupportedOperationException();
    }

    @Override
    public AMQP.Tx.RollbackOk txRollback() {
        throw new UnsupportedOperationException();
    }

    @Override
    public AMQP.Confirm.SelectOk confirmSelect() {
        throw new UnsupportedOperationException();
    }

    @Override
    public long getNextPublishSeqNo() {
        throw new UnsupportedOperationException();
    }

    @Override
    public boolean waitForConfirms() {
        throw new UnsupportedOperationException();
    }

    @Override
    public boolean waitForConfirms(long timeout) {
        throw new UnsupportedOperationException();
    }

    @Override
    public void waitForConfirmsOrDie() {
        throw new UnsupportedOperationException();
    }

    @Override
    public void waitForConfirmsOrDie(long timeout) {
        throw new UnsupportedOperationException();
    }

    @Override
    public void asyncRpc(Method method) {
        throw new UnsupportedOperationException();
    }

    @Override
    public Command rpc(Method method) {
        throw new UnsupportedOperationException();
    }

    @Override
    public long messageCount(String queue) {
        return node.messageCount(lastGeneratedIfEmpty(queue));
    }

    @Override
    public long consumerCount(String queue) {
        return node.consumerCount(lastGeneratedIfEmpty(queue));
    }

    @Override
    public CompletableFuture<Command> asyncCompletableRpc(Method method) {
        throw new UnsupportedOperationException();
    }

    @Override
    public void addShutdownListener(ShutdownListener listener) {
        throw new UnsupportedOperationException();
    }

    @Override
    public void removeShutdownListener(ShutdownListener listener) {
        throw new UnsupportedOperationException();
    }

    @Override
    public ShutdownSignalException getCloseReason() {
        throw new UnsupportedOperationException();
    }

    @Override
    public void notifyListeners() {
        throw new UnsupportedOperationException();
    }

    @Override
    public boolean isOpen() {
        return opened.get();
    }

    private AMQP.BasicProperties nullToEmpty(AMQP.BasicProperties props) {
        return props != null ? props : new AMQP.BasicProperties.Builder().build();
    }

    private Map<String, Object> nullToEmpty(Map<String, Object> arguments) {
        return arguments != null ? arguments : Collections.emptyMap();
    }

    private String generateIfEmpty(String queue) {
        final String definitiveQueueName;
        if ("".equals(queue)) {
            definitiveQueueName = queueNameGenerator.generate();
            this.lastGeneratedQueueName = definitiveQueueName;
        } else {
            definitiveQueueName = queue;
        }
        return definitiveQueueName;
    }

    private String lastGeneratedIfEmpty(String queue) {
        return "".equals(queue) ? Objects.requireNonNull(lastGeneratedQueueName, "No server-named queue previously created") : queue;
    }

    private long nextDeliveryTag() {
        return deliveryTagSequence.incrementAndGet();
    }
}
