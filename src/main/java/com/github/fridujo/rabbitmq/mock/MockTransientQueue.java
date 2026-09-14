package com.github.fridujo.rabbitmq.mock;

public class MockTransientQueue extends MockQueue {

    public MockTransientQueue(String name, AmqArguments arguments, ReceiverRegistry receiverRegistry) {
        super(name, arguments, receiverRegistry);
    }

    @Override
    public void basicCancel(String consumerTag) {
        super.basicCancel(consumerTag);
        if (consumerCount() == 0) {
            receiverRegistry.removeReceiver(this);
        }
    }

    @Override
    void close(MockConnection mockConnection) {
        boolean activeConsumers = consumerCount() > 0;
        super.close(mockConnection);
        if (activeConsumers && consumerCount() == 0) {
            receiverRegistry.removeReceiver(this);
        }
    }

    @Override
    public String toString() {
        return "Auto-Delete Queue " + pointer().name();
    }
}
