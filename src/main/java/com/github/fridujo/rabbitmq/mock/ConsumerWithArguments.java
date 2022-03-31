package com.github.fridujo.rabbitmq.mock;

import com.rabbitmq.client.Consumer;

import java.util.Map;

public interface ConsumerWithArguments extends Consumer {

    Map<String, Object> getArguments();
}
