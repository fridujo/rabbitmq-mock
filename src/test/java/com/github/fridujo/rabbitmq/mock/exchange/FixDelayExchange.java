package com.github.fridujo.rabbitmq.mock.exchange;

import java.util.Map;
import java.util.concurrent.TimeUnit;

import com.rabbitmq.client.AMQP;

import com.github.fridujo.rabbitmq.mock.AmqArguments;
import com.github.fridujo.rabbitmq.mock.ReceiverRegistry;

public class FixDelayExchange extends BindableMockExchange {

    public static final String TYPE = "x-fix-delayed-message";

    public FixDelayExchange(String name, AmqArguments arguments, ReceiverRegistry receiverRegistry) {
        super(name, TYPE, arguments, receiverRegistry);
    }

    @Override
    public void publish(String previousExchangeName, String routingKey, AMQP.BasicProperties props, byte[] body) {
        try {
            TimeUnit.SECONDS.sleep(1);
        } catch (InterruptedException e) {
        }
        super.publish(previousExchangeName, routingKey, props, body);
    }

    @Override
    protected boolean match(BindConfiguration bindConfiguration, String routingKey, Map<String, Object> headers) {
        return bindConfiguration.bindingKey.equals(routingKey);
    }
}
