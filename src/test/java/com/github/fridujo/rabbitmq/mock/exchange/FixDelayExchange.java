package com.github.fridujo.rabbitmq.mock.exchange;

import java.util.concurrent.TimeUnit;

import com.github.fridujo.rabbitmq.mock.AmqArguments;
import com.github.fridujo.rabbitmq.mock.ReceiverRegistry;
import com.rabbitmq.client.AMQP;

public class FixDelayExchange extends MockDirectExchange {

    public static final String TYPE = "x-fix-delayed-message";

    public FixDelayExchange(String name, AmqArguments arguments, ReceiverRegistry receiverRegistry) {
        super(name, arguments, receiverRegistry);
    }

    @Override
    public String getType() {
        return TYPE;
    }

    @Override
    public boolean publish(String previousExchangeName, String routingKey, AMQP.BasicProperties props, byte[] body) {
        try {
            TimeUnit.SECONDS.sleep(1);
        } catch (InterruptedException e) {
        }
        return super.publish(previousExchangeName, routingKey, props, body);
    }
}
