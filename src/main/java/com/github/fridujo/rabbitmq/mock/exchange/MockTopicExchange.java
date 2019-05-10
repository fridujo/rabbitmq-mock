package com.github.fridujo.rabbitmq.mock.exchange;

import java.util.Map;

import com.github.fridujo.rabbitmq.mock.AmqArguments;
import com.github.fridujo.rabbitmq.mock.ReceiverRegistry;

public class MockTopicExchange extends MultipleReceiverExchange {

    public static final String TYPE = "topic";

    public MockTopicExchange(String name, AmqArguments arguments, ReceiverRegistry receiverRegistry) {
        super(name, TYPE, arguments, receiverRegistry);
    }

    protected boolean match(BindConfiguration bindConfiguration, String routingKey, Map<String, Object> headers) {
        String bindingRegex = bindConfiguration.bindingKey
            .replace("*", "([^\\.]+)")
            .replace("#", "(.+)");
        return routingKey.matches(bindingRegex);
    }
}
