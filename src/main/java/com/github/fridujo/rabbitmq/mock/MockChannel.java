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
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.Collections;
import java.util.Map;
import java.util.UUID;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.atomic.AtomicBoolean;

public class MockChannel implements Channel {
    private static final Logger LOGGER = LoggerFactory.getLogger(MockChannel.class);

    private final int channelNumber;
    private final MockNode node;
    private final MockConnection mockConnection;
    private final AtomicBoolean opened = new AtomicBoolean(true);

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
        node.basicPublish(exchange, routingKey, mandatory, immediate, props, body);
    }

    @Override
    public AMQP.Exchange.DeclareOk exchangeDeclare(String exchange, String type) throws IOException {
        return exchangeDeclare(exchange, type, false, true, false, Collections.emptyMap());
    }

    @Override
    public AMQP.Exchange.DeclareOk exchangeDeclare(String exchange, BuiltinExchangeType type) throws IOException {
        return exchangeDeclare(exchange, type, false, true, false, Collections.emptyMap());
    }

    @Override
    public AMQP.Exchange.DeclareOk exchangeDeclare(String exchange, String type, boolean durable) throws IOException {
        return exchangeDeclare(exchange, type, durable, true, false, Collections.emptyMap());
    }

    @Override
    public AMQP.Exchange.DeclareOk exchangeDeclare(String exchange, BuiltinExchangeType type, boolean durable) throws IOException {
        return exchangeDeclare(exchange, type, durable, true, false, Collections.emptyMap());
    }

    @Override
    public AMQP.Exchange.DeclareOk exchangeDeclare(String exchange, String type, boolean durable, boolean autoDelete, Map<String, Object> arguments) throws IOException {
        return exchangeDeclare(exchange, type, durable, autoDelete, false, arguments);
    }

    @Override
    public AMQP.Exchange.DeclareOk exchangeDeclare(String exchange, BuiltinExchangeType type, boolean durable, boolean autoDelete, Map<String, Object> arguments) throws IOException {
        return exchangeDeclare(exchange, type, durable, autoDelete, false, arguments);
    }

    @Override
    public AMQP.Exchange.DeclareOk exchangeDeclare(String exchange, String type, boolean durable, boolean autoDelete, boolean internal, Map<String, Object> arguments) {
        return node.exchangeDeclare(exchange, type, durable, autoDelete, internal, arguments);
    }

    @Override
    public AMQP.Exchange.DeclareOk exchangeDeclare(String exchange, BuiltinExchangeType type, boolean durable, boolean autoDelete, boolean internal, Map<String, Object> arguments) throws IOException {
        return exchangeDeclare(exchange, type.getType(), durable, autoDelete, internal, arguments);
    }

    @Override
    public void exchangeDeclareNoWait(String exchange, String type, boolean durable, boolean autoDelete, boolean internal, Map<String, Object> arguments) {
        throw new UnsupportedOperationException();
    }

    @Override
    public void exchangeDeclareNoWait(String exchange, BuiltinExchangeType type, boolean durable, boolean autoDelete, boolean internal, Map<String, Object> arguments) {
        throw new UnsupportedOperationException();
    }

    @Override
    public AMQP.Exchange.DeclareOk exchangeDeclarePassive(String name) {
        throw new UnsupportedOperationException();
    }

    @Override
    public AMQP.Exchange.DeleteOk exchangeDelete(String exchange, boolean ifUnused) {
        throw new UnsupportedOperationException();
    }

    @Override
    public void exchangeDeleteNoWait(String exchange, boolean ifUnused) {
        throw new UnsupportedOperationException();
    }

    @Override
    public AMQP.Exchange.DeleteOk exchangeDelete(String exchange) {
        throw new UnsupportedOperationException();
    }

    @Override
    public AMQP.Exchange.BindOk exchangeBind(String destination, String source, String routingKey) {
        throw new UnsupportedOperationException();
    }

    @Override
    public AMQP.Exchange.BindOk exchangeBind(String destination, String source, String routingKey, Map<String, Object> arguments) {
        throw new UnsupportedOperationException();
    }

    @Override
    public void exchangeBindNoWait(String destination, String source, String routingKey, Map<String, Object> arguments) {
        throw new UnsupportedOperationException();
    }

    @Override
    public AMQP.Exchange.UnbindOk exchangeUnbind(String destination, String source, String routingKey) {
        throw new UnsupportedOperationException();
    }

    @Override
    public AMQP.Exchange.UnbindOk exchangeUnbind(String destination, String source, String routingKey, Map<String, Object> arguments) {
        throw new UnsupportedOperationException();
    }

    @Override
    public void exchangeUnbindNoWait(String destination, String source, String routingKey, Map<String, Object> arguments) {
        throw new UnsupportedOperationException();
    }

    @Override
    public AMQP.Queue.DeclareOk queueDeclare() {
        return node.queueDeclare(UUID.randomUUID().toString(), false, true, true, null);
    }

    @Override
    public AMQP.Queue.DeclareOk queueDeclare(String queue, boolean durable, boolean exclusive, boolean autoDelete, Map<String, Object> arguments) {
        return node.queueDeclare(queue, durable, exclusive, autoDelete, arguments);
    }

    @Override
    public void queueDeclareNoWait(String queue, boolean durable, boolean exclusive, boolean autoDelete, Map<String, Object> arguments) {
        throw new UnsupportedOperationException();
    }

    @Override
    public AMQP.Queue.DeclareOk queueDeclarePassive(String queue) throws IOException {
        if (!node.getQueue(queue).isPresent()) {
            throw new IOException("No queue named " + queue);
        }
        return null;
    }

    @Override
    public AMQP.Queue.DeleteOk queueDelete(String queue) {
        throw new UnsupportedOperationException();
    }

    @Override
    public AMQP.Queue.DeleteOk queueDelete(String queue, boolean ifUnused, boolean ifEmpty) {
        throw new UnsupportedOperationException();
    }

    @Override
    public void queueDeleteNoWait(String queue, boolean ifUnused, boolean ifEmpty) {
        throw new UnsupportedOperationException();
    }

    @Override
    public AMQP.Queue.BindOk queueBind(String queue, String exchange, String routingKey) {
        return node.queueBind(queue, exchange, routingKey, null);
    }

    @Override
    public AMQP.Queue.BindOk queueBind(String queue, String exchange, String routingKey, Map<String, Object> arguments) {
        return node.queueBind(queue, exchange, routingKey, arguments);
    }

    @Override
    public void queueBindNoWait(String queue, String exchange, String routingKey, Map<String, Object> arguments) {
        throw new UnsupportedOperationException();
    }

    @Override
    public AMQP.Queue.UnbindOk queueUnbind(String queue, String exchange, String routingKey) {
        throw new UnsupportedOperationException();
    }

    @Override
    public AMQP.Queue.UnbindOk queueUnbind(String queue, String exchange, String routingKey, Map<String, Object> arguments) {
        throw new UnsupportedOperationException();
    }

    @Override
    public AMQP.Queue.PurgeOk queuePurge(String queue) {
        throw new UnsupportedOperationException();
    }

    @Override
    public GetResponse basicGet(String queue, boolean autoAck) {
        return node.basicGet(queue, autoAck);
    }

    @Override
    public void basicAck(long deliveryTag, boolean multiple) {
        node.basicAck(deliveryTag);
    }

    @Override
    public void basicNack(long deliveryTag, boolean multiple, boolean requeue) {
        throw new UnsupportedOperationException();
    }

    @Override
    public void basicReject(long deliveryTag, boolean requeue) {
        throw new UnsupportedOperationException();
    }

    @Override
    public String basicConsume(String queue, Consumer callback) {
        throw new UnsupportedOperationException();
    }

    @Override
    public String basicConsume(String queue, DeliverCallback deliverCallback, CancelCallback cancelCallback) {
        throw new UnsupportedOperationException();
    }

    @Override
    public String basicConsume(String queue, DeliverCallback deliverCallback, ConsumerShutdownSignalCallback shutdownSignalCallback) {
        throw new UnsupportedOperationException();
    }

    @Override
    public String basicConsume(String queue, DeliverCallback deliverCallback, CancelCallback cancelCallback, ConsumerShutdownSignalCallback shutdownSignalCallback) {
        throw new UnsupportedOperationException();
    }

    @Override
    public String basicConsume(String queue, boolean autoAck, Consumer callback) {
        throw new UnsupportedOperationException();
    }

    @Override
    public String basicConsume(String queue, boolean autoAck, DeliverCallback deliverCallback, CancelCallback cancelCallback) {
        throw new UnsupportedOperationException();
    }

    @Override
    public String basicConsume(String queue, boolean autoAck, DeliverCallback deliverCallback, ConsumerShutdownSignalCallback shutdownSignalCallback) {
        throw new UnsupportedOperationException();
    }

    @Override
    public String basicConsume(String queue, boolean autoAck, DeliverCallback deliverCallback, CancelCallback cancelCallback, ConsumerShutdownSignalCallback shutdownSignalCallback) {
        throw new UnsupportedOperationException();
    }

    @Override
    public String basicConsume(String queue, boolean autoAck, Map<String, Object> arguments, Consumer callback) throws IOException {
        return basicConsume(queue, autoAck, "", false, false, arguments, callback);
    }

    @Override
    public String basicConsume(String queue, boolean autoAck, Map<String, Object> arguments, DeliverCallback deliverCallback, CancelCallback cancelCallback) {
        throw new UnsupportedOperationException();
    }

    @Override
    public String basicConsume(String queue, boolean autoAck, Map<String, Object> arguments, DeliverCallback deliverCallback, ConsumerShutdownSignalCallback shutdownSignalCallback) {
        throw new UnsupportedOperationException();
    }

    @Override
    public String basicConsume(String queue, boolean autoAck, Map<String, Object> arguments, DeliverCallback deliverCallback, CancelCallback cancelCallback, ConsumerShutdownSignalCallback shutdownSignalCallback) {
        throw new UnsupportedOperationException();
    }

    @Override
    public String basicConsume(String queue, boolean autoAck, String consumerTag, Consumer callback) throws IOException {
        return basicConsume(queue, autoAck, consumerTag, false, false, Collections.emptyMap(), callback);
    }

    @Override
    public String basicConsume(String queue, boolean autoAck, String consumerTag, DeliverCallback deliverCallback, CancelCallback cancelCallback) {
        throw new UnsupportedOperationException();
    }

    @Override
    public String basicConsume(String queue, boolean autoAck, String consumerTag, DeliverCallback deliverCallback, ConsumerShutdownSignalCallback shutdownSignalCallback) {
        throw new UnsupportedOperationException();
    }

    @Override
    public String basicConsume(String queue, boolean autoAck, String consumerTag, DeliverCallback deliverCallback, CancelCallback cancelCallback, ConsumerShutdownSignalCallback shutdownSignalCallback) {
        throw new UnsupportedOperationException();
    }

    @Override
    public String basicConsume(String queue, boolean autoAck, String consumerTag, boolean noLocal, boolean exclusive, Map<String, Object> arguments, Consumer callback) {
        return node.basicConsume(queue, autoAck, consumerTag, noLocal, exclusive, arguments, callback);
    }

    @Override
    public String basicConsume(String queue, boolean autoAck, String consumerTag, boolean noLocal, boolean exclusive, Map<String, Object> arguments, DeliverCallback deliverCallback, CancelCallback cancelCallback) {
        throw new UnsupportedOperationException();
    }

    @Override
    public String basicConsume(String queue, boolean autoAck, String consumerTag, boolean noLocal, boolean exclusive, Map<String, Object> arguments, DeliverCallback deliverCallback, ConsumerShutdownSignalCallback shutdownSignalCallback) {
        throw new UnsupportedOperationException();
    }

    @Override
    public String basicConsume(String queue, boolean autoAck, String consumerTag, boolean noLocal, boolean exclusive, Map<String, Object> arguments, DeliverCallback deliverCallback, CancelCallback cancelCallback, ConsumerShutdownSignalCallback shutdownSignalCallback) {
        throw new UnsupportedOperationException();
    }

    @Override
    public void basicCancel(String consumerTag) {
        // Called when BlockingQueueConsumer#basicCancel (after AbstractMessageListenerContainer#stop)
        LOGGER.info("Cancelled consumer " + consumerTag);
    }

    @Override
    public AMQP.Basic.RecoverOk basicRecover() {
        throw new UnsupportedOperationException();
    }

    @Override
    public AMQP.Basic.RecoverOk basicRecover(boolean requeue) {
        throw new UnsupportedOperationException();
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
        throw new UnsupportedOperationException();
    }

    @Override
    public long consumerCount(String queue) {
        throw new UnsupportedOperationException();
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
}
