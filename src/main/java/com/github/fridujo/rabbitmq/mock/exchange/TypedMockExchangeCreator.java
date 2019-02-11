package com.github.fridujo.rabbitmq.mock.exchange;

public interface TypedMockExchangeCreator extends MockExchangeCreator {

    String getType();
}
