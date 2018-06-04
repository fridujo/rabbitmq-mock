package com.github.fridujo.rabbitmq.mock;

import com.rabbitmq.client.AlreadyClosedException;
import com.rabbitmq.client.BlockedCallback;
import com.rabbitmq.client.BlockedListener;
import com.rabbitmq.client.Channel;
import com.rabbitmq.client.Connection;
import com.rabbitmq.client.ExceptionHandler;
import com.rabbitmq.client.ShutdownListener;
import com.rabbitmq.client.ShutdownSignalException;
import com.rabbitmq.client.UnblockedCallback;

import java.net.InetAddress;
import java.util.Map;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;

public class MockConnection implements Connection {

    private final AtomicBoolean opened = new AtomicBoolean(true);
    private final AtomicInteger channelSequence = new AtomicInteger();
    private final MockNode mockNode;

    public MockConnection(MockNode mockNode) {
        this.mockNode = mockNode;
    }

    @Override
    public InetAddress getAddress() {
        throw new UnsupportedOperationException();
    }

    @Override
    public int getPort() {
        throw new UnsupportedOperationException();
    }

    @Override
    public int getChannelMax() {
        throw new UnsupportedOperationException();
    }

    @Override
    public int getFrameMax() {
        throw new UnsupportedOperationException();
    }

    @Override
    public int getHeartbeat() {
        throw new UnsupportedOperationException();
    }

    @Override
    public Map<String, Object> getClientProperties() {
        throw new UnsupportedOperationException();
    }

    @Override
    public String getClientProvidedName() {
        throw new UnsupportedOperationException();
    }

    @Override
    public Map<String, Object> getServerProperties() {
        throw new UnsupportedOperationException();
    }

    @Override
    public Channel createChannel() throws AlreadyClosedException {
        if (!isOpen()) {
            throw new AlreadyClosedException(new ShutdownSignalException(false, true, null, this));
        }
        return createChannel(channelSequence.incrementAndGet());
    }

    @Override
    public Channel createChannel(int channelNumber) {
        MockChannel mockChannel = new MockChannel(channelNumber, mockNode, this);
        return mockChannel;
    }

    @Override
    public void close() {
        opened.set(false);
    }

    @Override
    public void close(int closeCode, String closeMessage) {
        opened.set(false);
    }

    @Override
    public void close(int timeout) {
        throw new UnsupportedOperationException();
    }

    @Override
    public void close(int closeCode, String closeMessage, int timeout) {
        throw new UnsupportedOperationException();
    }

    @Override
    public void abort() {
        throw new UnsupportedOperationException();
    }

    @Override
    public void abort(int closeCode, String closeMessage) {
        throw new UnsupportedOperationException();
    }

    @Override
    public void abort(int timeout) {
        throw new UnsupportedOperationException();
    }

    @Override
    public void abort(int closeCode, String closeMessage, int timeout) {
        throw new UnsupportedOperationException();
    }

    @Override
    public void addBlockedListener(BlockedListener listener) {
        throw new UnsupportedOperationException();
    }

    @Override
    public BlockedListener addBlockedListener(BlockedCallback blockedCallback, UnblockedCallback unblockedCallback) {
        throw new UnsupportedOperationException();
    }

    @Override
    public boolean removeBlockedListener(BlockedListener listener) {
        throw new UnsupportedOperationException();
    }

    @Override
    public void clearBlockedListeners() {
        throw new UnsupportedOperationException();
    }

    @Override
    public ExceptionHandler getExceptionHandler() {
        throw new UnsupportedOperationException();
    }

    @Override
    public String getId() {
        throw new UnsupportedOperationException();
    }

    @Override
    public void setId(String id) {
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
