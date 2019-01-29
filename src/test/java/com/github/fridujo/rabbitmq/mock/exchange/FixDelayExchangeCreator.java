package com.github.fridujo.rabbitmq.mock.exchange;

import java.util.Map;
import java.util.concurrent.TimeUnit;

import com.rabbitmq.client.AMQP;

import com.github.fridujo.rabbitmq.mock.AmqArguments;
import com.github.fridujo.rabbitmq.mock.ReceiverRegistry;

public class FixDelayExchangeCreator implements MockExchangeCreator {
    @Override
    public String getType() {
        return "x-fix-delayed-message";
    }

    @Override
    public BindableMockExchange createMockExchange(String exchangeName, AmqArguments arguments, ReceiverRegistry receiverRegistry) {
        return new FixDelayExchange(exchangeName, arguments, receiverRegistry);
    }

    static class FixDelayExchange extends BindableMockExchange {

        private FixDelayExchange(String name, AmqArguments arguments, ReceiverRegistry receiverRegistry) {
            super(name, arguments, receiverRegistry);
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
        protected boolean match(String bindingKey, Map<String, Object> bindArguments, String routingKey, Map<String, Object> headers) {
            return bindingKey.equals(routingKey);
        }
    }
}
